package com.hoccer.talk.server.rpc;

import com.hoccer.talk.model.*;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.ITalkServerStatistics;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.srp.SRP6Parameters;
import com.hoccer.talk.srp.SRP6VerifyingServer;
import com.hoccer.talk.util.MapUtil;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

/**
 * RPC handler for talk protocol communications
 * <p/>
 * Calls are exposed directly to the client, so essentially
 * this also has to take care of protocol-level security...
 * <p/>
 * This class does not hold any state except for login state.
 */
public class TalkRpcHandler implements ITalkRpcServer {

    private static final Logger LOG =
            Logger.getLogger(TalkRpcHandler.class);

    private static final Hex HEX = new Hex();
    private final Digest SRP_DIGEST = new SHA256Digest();
    private static final SecureRandom SRP_RANDOM = new SecureRandom();
    private static final SRP6Parameters SRP_PARAMETERS = SRP6Parameters.CONSTANTS_1024;
    private static final int TOKEN_LIFETIME_MIN = 60; // (seconds) at least 1 minute
    private static final int TOKEN_LIFETIME_MAX = 7 * 24 * 3600; // (seconds) at most 1 week
    private static final int TOKEN_MAX_USAGE = 1;
    private static final int PAIRING_TOKEN_MAX_USAGE_RANGE_MIN = 1;
    private static final int PAIRING_TOKEN_MAX_USAGE_RANGE_MAX = 50;


    /**
     * Reference to server
     */
    final private TalkServer mServer;

    /**
     * Reference to database accessor
     */
    final private ITalkServerDatabase mDatabase;

    /**
     * Reference to stats collector
     */
    final private ITalkServerStatistics mStatistics;

    /**
     * Reference to connection object
     */
    final private TalkRpcConnection mConnection;

    /**
     * SRP authentication state
     */
    private SRP6VerifyingServer mSrpServer;

    /**
     * SRP authenticating user
     */
    private TalkClient mSrpClient;

    public TalkRpcHandler(TalkServer pServer, TalkRpcConnection pConnection) {
        mServer = pServer;
        mConnection = pConnection;
        mDatabase = mServer.getDatabase();
        mStatistics = mServer.getStatistics();
    }

    private void requireIsNotOutdated() {
        if (mConnection.isLegacyMode()) {
            throw new RuntimeException("Client too old");
        }
    }

    private void requireIdentification() {
        requireIsNotOutdated();
        if (!mConnection.isLoggedIn()) {
            throw new RuntimeException("Not logged in");
        }
    }

    private void requirePastIdentification() {
        if (!mConnection.wasLoggedIn()) {
            throw new RuntimeException("Was not logged in");
        }
    }

    private void logCall(String message) {
        if (mServer.getConfiguration().getLogAllCalls() || mConnection.isSupportMode()) {
            LOG.info("[connectionId: '" + mConnection.getConnectionId() + "'] " + message);
        }
    }

    @Override
    public void bing() {
        logCall("bing()");
        requireIdentification();
    }

    @Override
    public void ready() {
        logCall("ready()");
        requireIdentification();
        mConnection.readyClient();
    }

    @Override
    public Date getTime() {
        logCall("getTime()");
        return new Date();
    }

    @Override
    public TalkServerInfo hello(TalkClientInfo clientInfo) {
        logCall("hello()");
        requireIdentification();

        String tag = clientInfo.getSupportTag();
        if (tag != null && !tag.isEmpty()) {
            if (tag.equals(mServer.getConfiguration().getSupportTag())) {
                mConnection.activateSupportMode();
            } else {
                LOG.info("[connectionId: '" + mConnection.getConnectionId() + "'] sent invalid support tag '" + tag + "'.");
            }
        } else {
            if (mConnection.isSupportMode()) {
                mConnection.deactivateSupportMode();
            }
        }

        TalkClientHostInfo clientHostInfo = new TalkClientHostInfo(clientInfo);
        clientHostInfo.setClientId(mConnection.getClientId());
        clientHostInfo.setServerTime(new Date());
        mDatabase.saveClientHostInfo(clientHostInfo);

        TalkServerInfo serverInfo = new TalkServerInfo();
        serverInfo.setServerTime(new Date());
        serverInfo.setSupportMode(mConnection.isSupportMode());
        serverInfo.setVersion(mServer.getConfiguration().getVersion());
        serverInfo.setCommitId(mServer.getConfiguration().getGitInfo().commitId);

        List<String> protcolVersions = TalkRpcConnectionHandler.getCurrentProtocolVersions();
        for (String protcolVersion : protcolVersions) {
            serverInfo.addProtocolVersion(protcolVersion);
        }

        return serverInfo;
    }

    @Override
    public String generateId() {
        logCall("generateId()");

        if (mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't register while logged in");
        }
        if (mConnection.isRegistering()) {
            throw new RuntimeException("Can't register more than one identity per connection");
        }

        String clientId = UUID.randomUUID().toString();
        mConnection.beginRegistration(clientId);
        return clientId;
    }

    @Override
    public String srpRegister(String verifier, String salt) {
        logCall("srpRegister(verifier: '" + verifier + "', salt: '" + salt + "')");

        if (mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't register while logged in");
        }

        String clientId = mConnection.getUnregisteredClientId();

        if (clientId == null) {
            throw new RuntimeException("You need to generate an id before registering");
        }

        // TODO: check verifier and salt for viability

        TalkClient client = new TalkClient();
        client.setClientId(clientId);
        client.setSrpSalt(salt);
        client.setSrpVerifier(verifier);
        client.setTimeRegistered(new Date());

        try {
            mDatabase.saveClient(client);
            mStatistics.signalClientRegisteredSucceeded();
        } catch (RuntimeException e) {
            mStatistics.signalClientRegisteredFailed();
            throw e;
        }
        return clientId;
    }

    @Override
    public String srpPhase1(String clientId, String A) {
        logCall("srpPhase1(clientId: '" + clientId + "', '" + A + "')");

        // check if we aren't logged in already
        if (mConnection.isLoggedIn()) {
            throw new RuntimeException("Can not authenticate while logged in");
        }
        try {

            // create SRP state
            if (mSrpServer == null) {
                mSrpServer = new SRP6VerifyingServer();
            } else {
                throw new RuntimeException("Can only attempt SRP once per connection");
            }

            // get client object
            mSrpClient = mDatabase.findClientById(clientId);
            if (mSrpClient == null) {
                throw new RuntimeException("No such client");  // must not change this string, is checked on client side
            }

            // verify SRP registration
            if (mSrpClient.getSrpVerifier() == null || mSrpClient.getSrpSalt() == null) {
                throw new RuntimeException("Not registered");   // must not change this string, is checked on client side
            }

            // parse the salt from DB
            byte[] salt;
            try {
                salt = (byte[]) HEX.decode(mSrpClient.getSrpSalt());
            } catch (DecoderException e) {
                throw new RuntimeException("Bad salt", e);
            }

            // initialize SRP state
            mSrpServer.initVerifiable(
                    SRP_PARAMETERS.N, SRP_PARAMETERS.g,
                    new BigInteger(mSrpClient.getSrpVerifier(), 16),
                    clientId.getBytes(),
                    salt,
                    SRP_DIGEST, SRP_RANDOM
            );

            // generate server credentials
            BigInteger credentials = mSrpServer.generateServerCredentials();

            // computer secret / verify client credentials
            try {
                mSrpServer.calculateSecret(new BigInteger(A, 16));
            } catch (CryptoException e) {
                throw new RuntimeException("Authentication failed", e);
            }
            mStatistics.signalClientLoginSRP1Succeeded();
            // return our credentials for the client
            return credentials.toString(16);
        } catch (RuntimeException e) {
            mStatistics.signalClientLoginSRP1Failed();
            throw e;
        }

    }

    @Override
    public String srpPhase2(String M1) {
        logCall("srpPhase2('" + M1 + "')");

        // check if we aren't logged in already
        if (mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't authenticate while logged in");
        }

        try {
            // verify we are in a good state to do phase2
            if (mSrpServer == null) {
                throw new RuntimeException("Need to perform phase 1 first");
            }
            if (mSrpClient == null) {
                throw new RuntimeException("Internal error in SRP phase 2");
            }

            // parse the string given by the client
            byte[] M1b;
            try {
                M1b = (byte[]) HEX.decode(M1);
            } catch (DecoderException e) {
                throw new RuntimeException(e);
            }

            // perform the verification
            byte[] M2;
            try {
                M2 = mSrpServer.verifyClient(M1b);
            } catch (CryptoException e) {
                throw new RuntimeException("Verification failed", e);
            }

            // we are now logged in
            mConnection.identifyClient(mSrpClient.getClientId());
            mStatistics.signalClientLoginSRP2Succeeded();
            // clear SRP state
            mSrpClient = null;
            mSrpServer = null;

            // return server evidence for client to check
            //        return Hex.encodeHexString(M2);
            return new String(Hex.encodeHex(M2));
        } catch (RuntimeException e) {
            mStatistics.signalClientLoginSRP2Failed();
            throw e;
        }

    }

    @Override
    public void registerGcm(String registeredPackage, String registrationId) {
        requireIdentification();
        logCall("registerGcm(registeredPackage: '" + registeredPackage + "', registrationId: '" + registrationId + "')");
        TalkClient client = mConnection.getClient();
        client.setGcmPackage(registeredPackage);
        client.setGcmRegistration(registrationId);
        mDatabase.saveClient(client);
    }

    @Override
    public void unregisterGcm() {
        requireIdentification();
        logCall("unregisterGcm()");
        TalkClient client = mConnection.getClient();
        client.setGcmPackage(null);
        client.setGcmRegistration(null);
        mDatabase.saveClient(client);
    }

    @Override
    public void registerApns(String registrationToken) {
        requireIdentification();
        logCall("registerApns(registrationToken: '" + registrationToken + "')");
        // APNS occasionally returns these for no good reason
        if (registrationToken.isEmpty()) {
            return;
        }
        TalkClient client = mConnection.getClient();
        client.setApnsToken(registrationToken);
        mDatabase.saveClient(client);
    }

    @Override
    public void unregisterApns() {
        requireIdentification();
        logCall("unregisterApns()");
        TalkClient client = mConnection.getClient();
        client.setApnsToken(null);
        mDatabase.saveClient(client);
    }

    @Override
    public void hintApnsUnreadMessage(int numUnreadMessages) {
        requireIdentification();
        logCall("hintApnsUnreadMessages('" + numUnreadMessages + "' unread messages)");
        TalkClient client = mConnection.getClient();
        client.setApnsUnreadMessages(numUnreadMessages);
        mDatabase.saveClient(client);
    }

    @Override
    public TalkRelationship[] getRelationships(Date lastKnown) {
        requireIdentification();
        logCall("getRelationships(lastKnown: '" + lastKnown + "')");

        List<TalkRelationship> relationships =
                mDatabase.findRelationshipsChangedAfter(mConnection.getClientId(), lastKnown);

        // build the result array
        TalkRelationship[] res = new TalkRelationship[relationships.size()];
        int idx = 0;
        for (TalkRelationship r : relationships) {
            res[idx++] = r;
        }
        return res;
    }

    @Override
    public void updatePresence(TalkPresence presence) {
        requireIdentification();
        logCall("updatePresence()");
        updatePresence(presence, null);
    }

    @Override
    public void modifyPresence(TalkPresence presence) {
        requireIdentification();
        logCall("modifyPresence(presence: '" + presence + "')");
        Set<String> fields = presence.nonNullFields();
        updatePresence(presence, fields);
    }

    private void updatePresence(TalkPresence presence, Set<String> fields) {
        // find existing presence or create one
        TalkPresence existing = mDatabase.findPresenceForClient(mConnection.getClientId());
        if (existing == null) {
            existing = new TalkPresence();
        }
        // update the presence with what we got
        existing.updateWith(presence, fields);
        existing.setClientId(mConnection.getClientId());
        existing.setTimestamp(new Date());
        if (fields != null) {
            fields.add(TalkPresence.FIELD_CLIENT_ID);
            // if we do not send time stamp updates on presenceModified, we are more conservative and cause a full presence sync after login
            // fields.add(TalkPresence.FIELD_TIMESTAMP);
        }
        if (fields == null || fields.contains(TalkPresence.FIELD_CONNECTION_STATUS)) {
            if (presence.isOffline()) {
                // client os lying about it's presence
                existing.setConnectionStatus(TalkPresence.CONN_STATUS_ONLINE);
            } else if (presence.isConnected()) {
                existing.setConnectionStatus(presence.getConnectionStatus());
            } else {
                LOG.error("undefined connectionStatus in presence:" + presence.getConnectionStatus());
                existing.setConnectionStatus(TalkPresence.CONN_STATUS_ONLINE);
            }
        }

        mDatabase.savePresence(existing);
        mServer.getUpdateAgent().requestPresenceUpdate(mConnection.getClientId(), fields);
    }

    @Override
    public TalkPresence[] getPresences(Date lastKnown) {
        requireIdentification();
        logCall("getPresences(lastKnown: '" + lastKnown + "')");

        List<TalkPresence> pres = mDatabase.findPresencesChangedAfter(mConnection.getClientId(), lastKnown);
        // update connection status and convert results to array
        TalkPresence[] res = new TalkPresence[pres.size()];
        for (int i = 0; i < res.length; i++) {
            TalkPresence p = pres.get(i);
            if (p.getConnectionStatus() == null) {
                p.setConnectionStatus(mServer.isClientConnected(p.getClientId())
                        ? TalkPresence.CONN_STATUS_ONLINE : TalkPresence.CONN_STATUS_OFFLINE);
            }
            res[i] = pres.get(i);
        }

        return res;
    }

    @Override
    public void updateKey(TalkKey key) {
        requireIdentification();
        logCall("updateKey()");
        if (mDatabase.findKey(mConnection.getClientId(), key.getKeyId()) != null) {
            return;
        }

        key.setClientId(mConnection.getClientId());
        key.setTimestamp(new Date());
        // TODO: should check if the content is ok
        mDatabase.saveKey(key);
    }

    @Override
    public boolean verifyKey(String keyId) {
        requireIdentification();
        logCall("verifyKey( keyId: '" + keyId + "')");

        TalkKey key = mDatabase.findKey(mConnection.getClientId(), keyId);
        if (key != null) {
            String storedKeyId = key.getKeyId();
            String realKeyId = key.calcKeyId();
            if (storedKeyId != null && storedKeyId.equals(keyId)) {
                if (storedKeyId.equals(realKeyId)) {
                    return true;
                }
            }
        }
        return false;
    }


    @Override
    public TalkKey getKey(String clientId, String keyId) {
        requireIdentification();
        logCall("getKey(clientId: '" + clientId + "', keyId: '" + keyId + "')");

        TalkKey key = null;

        TalkRelationship relationship = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if (relationship != null && (relationship.isFriend() || relationship.invitedMe())) {
            key = mDatabase.findKey(clientId, keyId);
        } else {
            List<TalkGroupMember> members = mDatabase.findGroupMembersForClient(mConnection.getClientId());
            for (TalkGroupMember member : members) {
                if (member.isJoined() || member.isInvited()) {
                    TalkGroupMember otherMember = mDatabase.findGroupMemberForClient(member.getGroupId(), clientId);
                    if (otherMember != null && (otherMember.isJoined() || otherMember.isInvited())) {
                        key = mDatabase.findKey(clientId, keyId);
                        break;
                    }
                }
            }
        }

        if (key == null) {
            throw new RuntimeException("Given client is not your friend, has invited you or key does not exist");
        }

        return key;
    }

    @Override
    public String generateToken(String tokenPurpose, int secondsValid) {
        requireIdentification();
        logCall("generateToken(tokenPurpose: '" + tokenPurpose + "', secondsValid: '" + secondsValid + "')");

        // verify request
        if (!TalkToken.isValidPurpose(tokenPurpose)) {
            throw new RuntimeException("Invalid token purpose");
        }

        // constrain validity period
        secondsValid = Math.max(TOKEN_LIFETIME_MIN, secondsValid);
        secondsValid = Math.min(TOKEN_LIFETIME_MAX, secondsValid);

        // get the current time
        Date time = new Date();
        // compute expiry time
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        calendar.add(Calendar.SECOND, Math.round(secondsValid));

        // generate the secret
        int attempt = 0;
        String secret = null;
        do {
            if (secret != null) {
                LOG.warn("Token generator returned existing token - regenerating");
            }
            if (attempt++ > 3) {
                throw new RuntimeException("Could not generate a token");
            }
            secret = genPw();
        } while (mDatabase.findTokenByPurposeAndSecret(tokenPurpose, secret) != null);

        // create the token object
        TalkToken token = new TalkToken();
        token.setClientId(mConnection.getClientId());
        token.setPurpose(tokenPurpose);
        token.setState(TalkToken.STATE_UNUSED);
        token.setSecret(secret);
        token.setGenerationTime(time);
        token.setExpiryTime(calendar.getTime());
        token.setUseCount(0);
        token.setMaxUseCount(TOKEN_MAX_USAGE);

        // save the token
        mDatabase.saveToken(token);

        // return the secret
        return token.getSecret();
    }

    @Override
    public String generatePairingToken(int maxUseCount, int secondsValid) {
        requireIdentification();

        logCall("generatePairingToken(maxUseCount: '" + maxUseCount + "', secondsValid: '" + secondsValid + "')");

        // constrain validity period
        secondsValid = Math.max(TOKEN_LIFETIME_MIN, secondsValid);
        secondsValid = Math.min(TOKEN_LIFETIME_MAX, secondsValid);

        // constrain use count
        maxUseCount = Math.max(PAIRING_TOKEN_MAX_USAGE_RANGE_MIN, maxUseCount);
        maxUseCount = Math.min(PAIRING_TOKEN_MAX_USAGE_RANGE_MAX, maxUseCount);

        // get the current time
        Date time = new Date();
        // compute expiry time
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(time);
        calendar.add(Calendar.SECOND, Math.round(secondsValid));

        // generate the secret
        int attempt = 0;
        String secret = null;
        do {
            if (secret != null) {
                LOG.warn("Token generator returned existing token - regenerating");
            }
            if (attempt++ > 3) {
                throw new RuntimeException("Could not generate a token");
            }
            secret = genPw();
        } while (mDatabase.findTokenByPurposeAndSecret(TalkToken.PURPOSE_PAIRING, secret) != null);

        // create the token object
        TalkToken token = new TalkToken();
        token.setClientId(mConnection.getClientId());
        token.setPurpose(TalkToken.PURPOSE_PAIRING);
        token.setState(TalkToken.STATE_UNUSED);
        token.setSecret(secret);
        token.setGenerationTime(time);
        token.setExpiryTime(calendar.getTime());
        token.setUseCount(0);
        token.setMaxUseCount(maxUseCount);

        // save the token
        mDatabase.saveToken(token);

        // return the secret
        return token.getSecret();
    }

    // TODO: extract as generic TokenGenerator for the server in general?!
    // TODO: Do not use a command line tool for this! Use a library or so...
    private String genPw() {
        String result = null;
        ProcessBuilder pb = new ProcessBuilder("pwgen", "10", "1");
        try {
            Process p = pb.start();
            InputStream s = p.getInputStream();
            BufferedReader r = new BufferedReader(new InputStreamReader(s));
            String line = r.readLine();
            LOG.debug("pwline " + line);
            if (line.length() == 10) {
                result = line;
            }
        } catch (IOException ioe) {
            LOG.error("Error in running 'pwgen'!", ioe);
        }
        return result;
    }

    @Override
    public boolean pairByToken(String secret) {
        requireIdentification();
        logCall("pairByToken(secret: '" + secret + "')");

        TalkToken token = mDatabase.findTokenByPurposeAndSecret(
                TalkToken.PURPOSE_PAIRING, secret);

        // check if token exists
        if (token == null) {
            LOG.info("no token could be found");
            return false;
        }

        // check if token is unused
        if (!token.isUsable()) {
            LOG.info("token can no longer be used");
            return false;
        }

        // check if token is still valid
        if (token.getExpiryTime().before(new Date())) {
            LOG.info("token has expired");
            return false;
        }

        // get relevant client IDs
        String myId = mConnection.getClientId();
        String otherId = token.getClientId();

        // reject self-pairing
        if (token.getClientId().equals(myId)) {
            LOG.info("self-pairing rejected");
            return false;
        }

        LOG.info("performing token-based pairing between clients with id '" + mConnection.getClientId() + "' and '" + token.getClientId() + "'");

        // set up relationships
        setRelationship(myId, otherId, TalkRelationship.STATE_FRIEND, true);
        setRelationship(otherId, myId, TalkRelationship.STATE_FRIEND, true);

        // invalidate the token
        token.setUseCount(token.getUseCount() + 1);
        if (token.getUseCount() < token.getMaxUseCount()) {
            token.setState(TalkToken.STATE_USED);
        } else {
            token.setState(TalkToken.STATE_SPENT);
        }
        mDatabase.saveToken(token);

        // give both users an initial presence
        mServer.getUpdateAgent().requestPresenceUpdateForClient(otherId, myId);
        mServer.getUpdateAgent().requestPresenceUpdateForClient(myId, otherId);

        return true;
    }

    @Override
    public void blockClient(String clientId) {
        requireIdentification();
        logCall("blockClient(id '" + clientId + "')");

        setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_BLOCKED, true);
    }

    @Override
    public void unblockClient(String clientId) {
        requireIdentification();
        logCall("unblockClient(id '" + clientId + "')");

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if (rel == null) {
            throw new RuntimeException("You are not paired with client with id '" + clientId + "'");
        }

        String oldState = rel.getState();
        if (TalkRelationship.STATE_FRIEND.equals(oldState)) {
            return;
        }
        if (TalkRelationship.STATE_BLOCKED.equals(oldState)) {
            setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_FRIEND, true);
            return;
        }

        throw new RuntimeException("You are not paired with client with id '" + clientId + "'");
    }

    @Override
    public void inviteFriend(String clientId) {
        requireIdentification();
        logCall("inviteFriend(id '" + clientId + "')");

        String myId = mConnection.getClientId();

        TalkRelationship rel = mDatabase.findRelationshipBetween(myId, clientId);
        TalkRelationship reverse_rel = mDatabase.findRelationshipBetween(clientId,myId);

        if (rel != null) {
            if (reverse_rel == null) {
                throw new RuntimeException("Server error: Relationship exists, but no reverse relationship found");
            }
            if (rel.isFriend()) {
                throw new RuntimeException("Contact is already a friend");
            }
            if (rel.isBlocked()) {
                throw new RuntimeException("Caller has blocked client, client must unblock contact first");
            }
            if (rel.invitedMe()) {
                throw new RuntimeException("Invited client has already invited me, must accept invitation");
            }
        }
        if (reverse_rel != null) {
            if (rel == null) {
                throw new RuntimeException("Server error: Reverse Relationship exists, but relationship to client not found");
            }
            if (reverse_rel.isBlocked()) {
                throw new RuntimeException("Contact has blocked client"); // TODO: leaks information about blocking to client, how shall we deal with it?
            }
            if (reverse_rel.isFriend()) {
                throw new RuntimeException("Server Error: Contact is already a friend, but not friend relationship to caller exists");
            }
            if (reverse_rel.isInvited()) {
                throw new RuntimeException("Server Error: Invited client has already invited me, but relationship does not have proper state");
            }
        }

        setRelationship(myId, clientId, TalkRelationship.STATE_INVITED, true);
        setRelationship(clientId, myId, TalkRelationship.STATE_INVITED_ME, true);

        // give only invited user a presence update from inviting client
        //mServer.getUpdateAgent().requestPresenceUpdateForClient(clientId, myId);
        mServer.getUpdateAgent().requestPresenceUpdateForClient(myId, clientId);
    }

    @Override
    public void disinviteFriend(String clientId) {
        requireIdentification();
        logCall("disinviteFriend(id '" + clientId + "')");

        String myId = mConnection.getClientId();

        TalkRelationship rel = mDatabase.findRelationshipBetween(myId, clientId);
        TalkRelationship reverse_rel = mDatabase.findRelationshipBetween(clientId,myId);

        if (rel == null) {
            throw new RuntimeException("No relationship exists with client with id '" + clientId + "'");
        }
        if (reverse_rel == null) {
            throw new RuntimeException("No reverse relationship exists with client with id '" + clientId + "'");
        }
        if (!rel.isInvited()) {
            throw new RuntimeException("Client with id '" + clientId + "'" +"is not invited");
        }
        setRelationship(myId, clientId, TalkRelationship.STATE_NONE, true);
        if (reverse_rel.invitedMe()) {
            setRelationship(clientId, myId, TalkRelationship.STATE_NONE, true);
        }
    }

    @Override
    public void acceptFriend(String clientId) {
        requireIdentification();
        logCall("acceptFriend(id '" + clientId + "')");

        String myId = mConnection.getClientId();

        TalkRelationship rel = mDatabase.findRelationshipBetween(myId, clientId);
        TalkRelationship reverse_rel = mDatabase.findRelationshipBetween(clientId,myId);

        if (rel == null) {
            throw new RuntimeException("No relationship exists with client with id '" + clientId + "'");
        }
        if (reverse_rel == null) {
            throw new RuntimeException("No reverse relationship exists with client with id '" + clientId + "'");
        }
        if (!rel.invitedMe()) {
            throw new RuntimeException("Relationship to client with id '" + clientId + "'" +"is not in invitedMe state");
        }
        if (!reverse_rel.isInvited()) {
            throw new RuntimeException("Relationship from client with id '" + clientId + "'" +"is not in invited state");
        }
        setRelationship(clientId, myId, TalkRelationship.STATE_FRIEND, true);
        setRelationship(myId, clientId, TalkRelationship.STATE_FRIEND, true);

        // send each other current presence to new befriended pair
        mServer.getUpdateAgent().requestPresenceUpdateForClient(clientId, myId);
        mServer.getUpdateAgent().requestPresenceUpdateForClient(myId, clientId);
    }

    @Override
    public void refuseFriend(String clientId) {
        requireIdentification();
        logCall("acceptFriend(id '" + clientId + "')");

        String myId = mConnection.getClientId();

        TalkRelationship rel = mDatabase.findRelationshipBetween(myId, clientId);
        TalkRelationship reverse_rel = mDatabase.findRelationshipBetween(clientId,myId);

        if (rel == null) {
            throw new RuntimeException("No relationship exists with client with id '" + clientId + "'");
        }
        if (reverse_rel == null) {
            throw new RuntimeException("No reverse relationship exists with client with id '" + clientId + "'");
        }
        if (!rel.invitedMe()) {
            throw new RuntimeException("Relationship to client with id '" + clientId + "'" +"is not in invitedMe state");
        }
        if (!reverse_rel.isInvited()) {
            throw new RuntimeException("Relationship from client with id '" + clientId + "'" +"is not in invited state");
        }
        setRelationship(clientId, myId, TalkRelationship.STATE_NONE, true);
        setRelationship(myId, clientId, TalkRelationship.STATE_NONE, true);
    }

    @Override
    public void depairClient(String clientId) {
        requireIdentification();

        logCall("depairClient(id '" + clientId + "')");

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if (rel == null) {
            return;
        }

        setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_NONE, true);
        setRelationship(clientId, mConnection.getClientId(), TalkRelationship.STATE_NONE, true);
    }

    private void setRelationship(String thisClientId, String otherClientId, String state, boolean notify) {
        if (!TalkRelationship.isValidState(state)) {
            throw new RuntimeException("Invalid state '" + state + "'");
        }
        TalkClient otherClient = mDatabase.findClientById(otherClientId);
        if (otherClient == null) {
            throw new RuntimeException("Invalid client to relate to - does not exist!");
        }

        TalkRelationship relationship = mDatabase.findRelationshipBetween(thisClientId, otherClientId);
        if (relationship == null) {
            relationship = new TalkRelationship();
        }
        final String oldState = relationship.getState();

        relationship.setClientId(thisClientId);
        relationship.setOtherClientId(otherClientId);
        relationship.setState(state);
        relationship.setLastChanged(new Date());

        // always save and notify
        mDatabase.saveRelationship(relationship);
        LOG.info("relationship between clients with id '" + thisClientId + "' and '" + otherClientId + "' is now in state '" + state + "'");
        if (notify) {
            mServer.getUpdateAgent().requestRelationshipUpdate(relationship);
        }
    }

    @Override
    public TalkDelivery[] outDeliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();
        logCall("outDeliveryRequest(" + deliveries.length + " deliveries)");

        String clientId = mConnection.getClientId();
        if (clientId == null) {
            LOG.error("outDeliveryRequest null clientId on connection: '" + mConnection.getConnectionId() + "', address " + mConnection.getRemoteAddress());
        }
        // generate and assign message id
        String messageId = UUID.randomUUID().toString();
        message.setMessageId(messageId);

        // guarantee correct sender
        message.setSenderId(clientId);

        // walk deliveries and determine which to accept,
        // filling in missing things as we go
        Vector<TalkDelivery> acceptedDeliveries = new Vector<TalkDelivery>();
        Vector<TalkDelivery> resultDeliveries = new Vector<TalkDelivery>();
        for (TalkDelivery delivery : deliveries) {
            // fill out various fields
            delivery.ensureDates();
            delivery.setMessageId(message.getMessageId());
            delivery.setSenderId(clientId);
            if (message.getAttachmentFileId() == null) {
                delivery.setAttachmentState(TalkDelivery.ATTACHMENT_STATE_NONE);
            } else {
                if (delivery.getAttachmentState() == null) {
                    delivery.setAttachmentState(TalkDelivery.ATTACHMENT_STATE_NEW);
                }
            }

            Vector<TalkDelivery> processedDeliveries  = processNewDelivery(message, delivery);

            for (TalkDelivery processedDelivery : processedDeliveries) {
                // delivery will be returned as result, so mark outgoing time
                processedDelivery.setTimeUpdatedOut(new Date(processedDelivery.getTimeChanged().getTime() + 1));
                acceptedDeliveries.add(processedDelivery);

                TalkDelivery resultDelivery = new TalkDelivery();
                resultDelivery.updateWith(processedDelivery, TalkDelivery.REQUIRED_OUT_RESULT_FIELDS_SET);
                resultDeliveries.add(resultDelivery);
            }
         }

        // update number of deliveries
        message.setNumDeliveries(acceptedDeliveries.size());

        // process all accepted deliveries
        if (!acceptedDeliveries.isEmpty()) {
            // save deliveries first so messages get collected
            for (TalkDelivery ds : acceptedDeliveries) {
                mDatabase.saveDelivery(ds);
            }
            mDatabase.saveMessage(message);
            // initiate delivery for all recipients
            for (TalkDelivery ds : acceptedDeliveries) {
                if (!ds.isFailure()) {
                    mServer.getDeliveryAgent().requestDelivery(ds.getReceiverId(), false);
                }
            }
        }

        mStatistics.signalMessageAcceptedSucceeded();
        return resultDeliveries.toArray(new TalkDelivery[0]);
    }


    // this function checks if a message can be delivered and returns one or more deliveries
    // with the apropriate states.
    // If delivery is a client delivery, only one delivery is returned
    // if delivery is a group delivery, one delivery for each group member will be returned

    private Vector<TalkDelivery> processNewDelivery(TalkMessage message, TalkDelivery delivery) {
        Vector<TalkDelivery> result = new Vector<TalkDelivery>();

        Date currentDate = new Date();
        String senderId = mConnection.getClientId();

        if (!delivery.hasValidRecipient()) {
            LOG.info("delivery rejected: no valid recipient (neither group nor client delivery)");
            delivery.setState(TalkDelivery.STATE_FAILED);
            delivery.setReason("no valid recipient (neither group nor client delivery)");
        }

        if (delivery.isGroupDelivery()) {
            String groupId = delivery.getGroupId();

            // check that group exists
            TalkGroup group = mDatabase.findGroupById(groupId);
            if (group == null) {
                LOG.info("delivery rejected: no such group");
                delivery.setState(TalkDelivery.STATE_FAILED);
                delivery.setReason("no such group");
            } else {
                // check that sender is member of group
                TalkGroupMember clientMember = mDatabase.findGroupMemberForClient(groupId, senderId);
                if (clientMember == null || !clientMember.isMember()) {
                    LOG.info("delivery rejected: sender is not group member");
                    delivery.setState(TalkDelivery.STATE_FAILED);
                    delivery.setReason("sender is not group member");
                }
            }

            if (!TalkDelivery.STATE_FAILED.equals(delivery.getState())) {
                // deliver to each group member
                List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                for (TalkGroupMember member : members) {
                    if (member.getClientId().equals(senderId)) {
                        continue;
                    }
                    if (!member.isJoined()) {
                        continue;
                    }

                    TalkDelivery memberDelivery = new TalkDelivery(true);
                    memberDelivery.setMessageId(message.getMessageId());
                    memberDelivery.setMessageTag(delivery.getMessageTag());
                    memberDelivery.setGroupId(groupId);
                    memberDelivery.setSenderId(senderId);
                    memberDelivery.setKeyId(member.getMemberKeyId());
                    memberDelivery.setKeyCiphertext(member.getEncryptedGroupKey());
                    memberDelivery.setReceiverId(member.getClientId());
                    memberDelivery.setAttachmentState(delivery.getAttachmentState());
                    memberDelivery.setTimeAccepted(currentDate);

                    if (member.getEncryptedGroupKey() == null) {
                        LOG.warn("have no group key, discarding group message " + message.getMessageId() + " for client " + member.getClientId() + " group " + groupId);
                        memberDelivery.setState(TalkDelivery.STATE_FAILED);
                        memberDelivery.setReason("no group key for receiver available");
                    } else if (member.getSharedKeyId() != null && message.getSharedKeyId() != null && !member.getSharedKeyId().equals(message.getSharedKeyId())) {
                        LOG.warn("message key id and member shared key id mismatch, discarding group message " + message.getMessageId() + " for client " + member.getClientId() + " group " + groupId);
                        LOG.warn("message.sharedKeyId=" + message.getSharedKeyId() + " member.sharedKeyId= " + member.getSharedKeyId() + " group.sharedKeyId= " + group.getSharedKeyId());
                        memberDelivery.setState(TalkDelivery.STATE_FAILED);
                        memberDelivery.setReason("group key for receiver is not current");
                    } else {

                        boolean success = checkOneDelivery(message, memberDelivery);
                        if (success) {
                            memberDelivery.setState(TalkDelivery.STATE_DELIVERING);
                            LOG.info("delivering message " + message.getMessageId() + " for client " + member.getClientId() + " group " + groupId + " sharedKeyId=" + message.getSharedKeyId() + ", member sharedKeyId=" + member.getSharedKeyId());
                        } else {
                            LOG.info("failed message " + message.getMessageId() + " for client " + member.getClientId() + " group " + groupId + " sharedKeyId=" + message.getSharedKeyId() + ", member sharedKeyId=" + member.getSharedKeyId());
                        }
                    }
                    // set delivery timestamps
                    memberDelivery.setTimeAccepted(currentDate);
                    memberDelivery.setTimeChanged(currentDate);
                    mDatabase.saveDelivery(memberDelivery);
                    result.add(memberDelivery);
                }
                return result;
            }
        } else if (delivery.isClientDelivery()) {
            /*
            1) if a client is blocked we are done - message delivery is disallowed.
            2) Otherwise check if the sender has a valid relationship to the recipient that allows message delivery
            */
            String recipientId = delivery.getReceiverId();
            final TalkRelationship relationship = mDatabase.findRelationshipBetween(recipientId, senderId);

            if (isBlocking(relationship)) {
                LOG.info("Recipient: '" + recipientId + "' blocks sender: '" + senderId + "' -> Blocking delivery");
                delivery.setState(TalkDelivery.STATE_FAILED);
                delivery.setReason("recipient blocked sender");
            } else if (areBefriended(relationship, recipientId, senderId) ||
                    areRelatedViaGroupMembership(senderId, recipientId))  // we need this for nearby group direct messages
            {
                // check for even more failure reasons
                if (checkOneDelivery(message, delivery)) {
                    // mark delivery as in progress if everything is fine
                    delivery.setState(TalkDelivery.STATE_DELIVERING);
                }
            } else {
                LOG.info("Message delivery rejected since no permissive relationship via group or friendship exists. (" + senderId + ", " + recipientId + ")");
                delivery.setState(TalkDelivery.STATE_FAILED);
                delivery.setReason("neither friends nor joint group members");
            }
        }
        delivery.setTimeAccepted(currentDate);
        delivery.setTimeChanged(currentDate);
        result.add(delivery);

        return result;
    }

    private boolean areRelatedViaGroupMembership(String clientId1, String clientId2) {
        final List<TalkGroupMember> client1Groupmembers = mDatabase.findGroupMembersForClient(clientId1);
        final List<String> client1GroupIds = new ArrayList<String>();
        for (TalkGroupMember groupMember : client1Groupmembers) {
            LOG.debug("  * client1 membership state in group: '" + groupMember.getGroupId() + "':" + groupMember.getState());
            if (groupMember.isJoined()) {
                client1GroupIds.add(groupMember.getGroupId());
            }
        }

        final List<TalkGroupMember> client2Groupmembers = mDatabase.findGroupMembersForClient(clientId2);
        for (TalkGroupMember groupMember : client2Groupmembers) {
            LOG.debug("  * client2 membership state in group: '" + groupMember.getGroupId() + "':" + groupMember.getState());
            if (client1GroupIds.indexOf(groupMember.getGroupId()) != -1 &&
                    groupMember.isJoined()) {
                LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' are both joined in group! (groupId: '" + groupMember.getGroupId() + "')");
                return true;
            }
        }

        LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' are NOT both joined in the same group");
        return false;
    }

    private boolean isBlocking(TalkRelationship relationship) {
        // Check if recipient marked his relationship to the sender as BLOCKED
        return relationship != null && relationship.isBlocked();
    }

    private boolean areBefriended(TalkRelationship relationship, String clientId1, String clientId2) {
        // reject if there is no relationship
        if (relationship == null) {
            LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' have no relationship with each other");
            return false;
        }

        // reject unless befriended
        if (!TalkRelationship.STATE_FRIEND.equals(relationship.getState())) {
            LOG.info("clients '" + clientId1 + "' and '" + clientId2 +
                    "' are not friends (relationship is '" + relationship.getState() + "')");
            return false;
        }

        LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' are friends!");
        return true;
    }

    private boolean checkOneDelivery(TalkMessage m, TalkDelivery delivery) {
        // who is doing this again?
        String clientId = mConnection.getClientId();
        // get the receiver
        String receiverId = delivery.getReceiverId();

        // reject messages to self
        if (receiverId.equals(clientId)) {
            LOG.info("delivery rejected: send to self");
            // mark delivery failed
            delivery.setState(TalkDelivery.STATE_FAILED);
            delivery.setReason("send to self");
            return false;
        }

        // reject messages to nonexisting clients
        TalkClient receiver = mDatabase.findClientById(receiverId);
        if (receiver == null) {
            LOG.info("delivery rejected: recipient with id '" + receiverId + "' does not exist");
            // mark delivery failed
            delivery.setState(TalkDelivery.STATE_FAILED);
            delivery.setReason("recipient does not exist");
            return false;
        }

        // all fine, delivery accepted
        LOG.info("delivery accepted for recipient with id '" + receiverId + "'");
        // return
        return true;
    }

    private TalkDelivery inDeliveryConfirm(String messageId, String confirmationState) {
        requireIdentification();
        synchronized (mServer.idLock(messageId)) {
            String clientId = mConnection.getClientId();
            TalkDelivery delivery = mDatabase.findDelivery(messageId, clientId);
            if (delivery != null) {
                if (delivery.nextStateAllowed(confirmationState)) {
                    LOG.info("confirmed '"+confirmationState+"' message with id '" + messageId + "' for client with id '" + clientId + "'");
                    setDeliveryState(delivery, confirmationState, true, false);
                    mStatistics.signalMessageConfirmedSucceeded();
                } else {
                    throw new RuntimeException("deliveryConfirm: no state change path to '"+confirmationState+"' from current delivery state ="+delivery.getState()+") : message id '" + messageId + "' client id '" + clientId + "'");
                }
                TalkDelivery result = new TalkDelivery();
                result.updateWith(delivery, TalkDelivery.REQUIRED_IN_UPDATE_FIELDS_SET);
                return result;
            } else {
                throw new RuntimeException("deliveryConfirm '"+confirmationState+"': no delivery found for message with id '" + messageId + "' for client with id '" + clientId + "'");
            }
        }
    }

    @Override
    public TalkDelivery inDeliveryConfirmUnseen(String messageId) {
        logCall("inDeliveryConfirmUnseen(messageId: '" + messageId + "')");
        return inDeliveryConfirm(messageId, TalkDelivery.STATE_DELIVERED_UNSEEN);
    }

    @Override
    public TalkDelivery inDeliveryConfirmSeen(String messageId) {
        logCall("inDeliveryConfirmSeen(messageId: '" + messageId + "')");
        return inDeliveryConfirm(messageId, TalkDelivery.STATE_DELIVERED_SEEN);
    }

    @Override
    public TalkDelivery inDeliveryConfirmPrivate(String messageId) {
        logCall("inDeliveryConfirmPrivate(messageId: '" + messageId + "')");
        return inDeliveryConfirm(messageId, TalkDelivery.STATE_DELIVERED_PRIVATE);
    }

    private TalkDelivery outDeliveryAcknowledge(String messageId, String recipientId, String acknowledgeState, String acknowledgedState) {
        requireIdentification();
        logCall("deliveryAcknowledge '"+acknowledgeState+"' (messageId: '" + messageId + "', recipientId: '" + recipientId + "')");
        synchronized (mServer.idLock(messageId)) {
            TalkDelivery delivery = mDatabase.findDelivery(messageId, recipientId);
            if (delivery != null) {
                String state = delivery.getState();
                if (acknowledgeState.equals(state) || acknowledgedState.equals(state)) {
                    LOG.info("acknowledged '"+acknowledgeState+"' message with id '" + messageId + "' for recipient with id '" + recipientId + "'");
                    setDeliveryState(delivery, acknowledgedState , false, true);
                    mStatistics.signalMessageAcknowledgedSucceeded();
                }  else {
                    LOG.error("deliveryAcknowledge '"+acknowledgeState+"' received for delivery not in state 'delivered' (state =" + delivery.getState() + ") : message id '" + messageId + "' recipientId '" + recipientId + "'");
                }
            }  else {
                LOG.error("deliveryAcknowledge '"+acknowledgeState+"' : no delivery found for message with id '" + messageId + "' for recipient with id '" + recipientId + "'");
            }
            TalkDelivery result = new TalkDelivery();
            result.updateWith(delivery, TalkDelivery.REQUIRED_OUT_UPDATE_FIELDS_SET);
            return result;
        }
    }

    @Override
    public TalkDelivery outDeliveryAcknowledgeUnseen(String messageId, String recipientId) {
        return outDeliveryAcknowledge(messageId, recipientId, TalkDelivery.STATE_DELIVERED_UNSEEN, TalkDelivery.STATE_DELIVERED_UNSEEN_ACKNOWLEDGED);
    }
    @Override
    public TalkDelivery outDeliveryAcknowledgeSeen(String messageId, String recipientId) {
        return outDeliveryAcknowledge(messageId, recipientId, TalkDelivery.STATE_DELIVERED_SEEN, TalkDelivery.STATE_DELIVERED_SEEN_ACKNOWLEDGED);
    }
    @Override
    public TalkDelivery outDeliveryAcknowledgePrivate(String messageId, String recipientId) {
        return outDeliveryAcknowledge(messageId, recipientId, TalkDelivery.STATE_DELIVERED_PRIVATE, TalkDelivery.STATE_DELIVERED_PRIVATE_ACKNOWLEDGED);
    }


    private TalkDelivery deliverySenderChangeState(String messageId, String recipientId, String newState) {
        synchronized (mServer.idLock(messageId)) {
            String clientId = mConnection.getClientId();
            TalkDelivery delivery = mDatabase.findDelivery(messageId, recipientId);
            if (delivery != null) {
                // abort outgoing delivery if we are the actual sender
                if (delivery.getSenderId().equals(clientId)) {
                    setDeliveryState(delivery, newState, false, true);
                } else {
                    throw new RuntimeException("you are not the sender");
                }
                TalkDelivery result = new TalkDelivery();
                result.updateWith(delivery, TalkDelivery.REQUIRED_OUT_UPDATE_FIELDS_SET);
                return result;
            } else {
                throw new RuntimeException("no delivery found for message with id '" + messageId + "' for recipient with id '" + recipientId + "'");
            }
        }
    }

    @Override
    public TalkDelivery outDeliveryAbort(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAbort(messageId: '" + messageId + "', recipientId: '" + recipientId + "'");
        return deliverySenderChangeState(messageId, recipientId, TalkDelivery.STATE_ABORTED_ACKNOWLEDGED);
    }

    @Override
    public TalkDelivery outDeliveryAcknowledgeRejected(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAbort(messageId: '" + messageId + "', recipientId: '" + recipientId + "'");
        return deliverySenderChangeState(messageId, recipientId, TalkDelivery.STATE_REJECTED_ACKNOWLEDGED);
    }
    @Override
    public TalkDelivery outDeliveryAcknowledgeFailed(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAbort(messageId: '" + messageId + "', recipientId: '" + recipientId + "'");
        return deliverySenderChangeState(messageId, recipientId, TalkDelivery.STATE_FAILED_ACKNOWLEDGED);
    }

    @Override
    public TalkDelivery inDeliveryReject(String messageId, String reason) {
        requireIdentification();
        logCall("deliveryReject(messageId: '" + messageId+", reason:"+reason);
        synchronized (mServer.idLock(messageId)) {
            String clientId = mConnection.getClientId();
            TalkDelivery delivery = mDatabase.findDelivery(messageId, clientId);
            if (delivery != null) {
                delivery.setReason(reason);
                setDeliveryState(delivery, TalkDelivery.STATE_REJECTED, true, false);
                TalkDelivery result = new TalkDelivery();
                result.updateWith(delivery, TalkDelivery.REQUIRED_OUT_UPDATE_FIELDS_SET);
                return result;
            } else {
                throw new RuntimeException("deliveryReject(): no delivery found for message with id '" + messageId + "' for recipient with id '" + clientId + "'");
            }
        }
    }

    private void setDeliveryState(TalkDelivery delivery, String state, boolean willReturnDeliveryIn, boolean willReturnDeliveryOut) {
        if (delivery.nextStateAllowed(state)) {

            delivery.setState(state);
            delivery.setTimeChanged(new Date());
            if (willReturnDeliveryIn) {
                delivery.setTimeUpdatedIn(new Date(delivery.getTimeChanged().getTime() + 1 ));
            }
            if (willReturnDeliveryOut) {
                delivery.setTimeUpdatedOut(new Date(delivery.getTimeChanged().getTime() + 1 ));
            }
            mDatabase.saveDelivery(delivery);
            if (TalkDelivery.OUT_STATES_SET.contains(state)) {
                mServer.getDeliveryAgent().requestDelivery(delivery.getSenderId(), false);
            } else if (TalkDelivery.IN_STATES_SET.contains(state)) {
                mServer.getDeliveryAgent().requestDelivery(delivery.getReceiverId(), false);
            } else if (delivery.isFinished()) {
                mServer.getCleaningAgent().cleanFinishedDelivery(delivery);
            }
        } else {
            throw new RuntimeException("Setting delivery state from '"+delivery.getState()+" to '"+state+"'not allowed");
        }
    }

    @Override
    public String createGroup(TalkGroup group) {
        requireIdentification();
        logCall("createGroup(groupTag: '" + group.getGroupTag() + "')");
        group.setGroupId(UUID.randomUUID().toString());
        group.setState(TalkGroup.STATE_EXISTS);
        TalkGroupMember groupAdmin = new TalkGroupMember();
        groupAdmin.setClientId(mConnection.getClientId());
        groupAdmin.setGroupId(group.getGroupId());
        groupAdmin.setRole(TalkGroupMember.ROLE_ADMIN);
        groupAdmin.setState(TalkGroupMember.STATE_JOINED);
        changedGroup(group, new Date());
        changedGroupMember(groupAdmin, group.getLastChanged(), true);
        return group.getGroupId();
    }

    @Override
    public TalkGroup[] getGroups(Date lastKnown) {
        requireIdentification();
        logCall("getGroups(lastKnown: '" + lastKnown + "')");
        List<TalkGroup> groups = mDatabase.findGroupsByClientIdChangedAfter(mConnection.getClientId(), lastKnown);
        TalkGroup[] res = new TalkGroup[groups.size()];
        for (int i = 0; i < res.length; i++) {
            res[i] = groups.get(i);
        }
        return res;
    }

    @Override
    public TalkGroup getGroup(String groupId) {
        requireIdentification();
        logCall("getGroup(id: '" + groupId + "')");
        return mDatabase.findGroupById(groupId);
    }

    @Override
    public TalkGroupMember getGroupMember(String groupId, String clientId) {
        requireIdentification();
        logCall("getGroupMember(groupId: '" + groupId + ", clientId:" + clientId + "')");
        return mDatabase.findGroupMemberForClient(groupId, clientId);
    }

    @Override
    public void updateGroupName(String groupId, String name) {
        requireIdentification();
        requireGroupAdmin(groupId);
        //requireNotNearbyGroupType(groupId);
        logCall("updateGroupName(groupId: '" + groupId + "', name: '" + name + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(groupId);
        targetGroup.setGroupName(name);
        changedGroup(targetGroup, new Date());
    }

    @Override
    public void updateGroupAvatar(String groupId, String avatarUrl) {
        requireIdentification();
        requireGroupAdmin(groupId);
        //requireNotNearbyGroupType(groupId);
        logCall("updateGroupAvatar(groupId: '" + groupId + "', avatarUrl: '" + avatarUrl + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(groupId);
        targetGroup.setGroupAvatarUrl(avatarUrl);
        changedGroup(targetGroup, new Date());
    }

    @Override
    public void updateGroup(TalkGroup group) {
        requireIdentification();
        requireGroupAdmin(group.getGroupId());
        logCall("updateGroup(groupId: '" + group.getGroupId() + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(group.getGroupId());
        targetGroup.setGroupName(group.getGroupName());
        targetGroup.setGroupAvatarUrl(group.getGroupAvatarUrl());
        changedGroup(targetGroup, new Date());
    }

    @Override
    public void deleteGroup(String groupId) {
        requireIdentification();
        requireGroupAdmin(groupId);
        logCall("deleteGroup(groupId: '" + groupId + "')");

        TalkGroup group = mDatabase.findGroupById(groupId);
        if (group == null) {
            throw new RuntimeException("Group does not exist");
        }

        // mark the group as deleted
        group.setState(TalkGroup.STATE_NONE);
        changedGroup(group, new Date());

        // walk the group and make everyone have a "none" relationship to it
        List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
        for (TalkGroupMember member : members) {
            if (member.isInvited() || member.isJoined()) {
                member.setState(TalkGroupMember.STATE_GROUP_REMOVED);

                // TODO: check if degrade role of admins is advisable
                /*if (member.isAdmin()) {
                    member.setRole(TalkGroupMember.ROLE_MEMBER);
                }*/
                changedGroupMember(member, group.getLastChanged(), false);
            }
        }
    }

    // touch presences of all members
    private void touchGroupMemberPresences(String groupId) {
        List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
        for (TalkGroupMember m : members) {
            if (m.isInvited() || m.isJoined()) {
                TalkPresence p = mDatabase.findPresenceForClient(m.getClientId());
                if (p != null) {
                    p.setTimestamp(new Date());
                    mDatabase.savePresence(p);
                }
            }
        }
    }

    @Override
    public void inviteGroupMember(String groupId, String clientId) {
        requireIdentification();
        requireGroupAdmin(groupId);
        //requireNotNearbyGroupType(groupId);
        logCall("inviteGroupMember(groupId: '" + groupId + "' / clientId: '" + clientId + "')");

        // check that the client exists
        TalkClient client = mDatabase.findClientById(clientId);
        if (client == null) {
            throw new RuntimeException("No such client");
        }
        // XXX need to apply blocklist here?
        // get or create the group member
        TalkGroupMember member = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (member == null) {
            member = new TalkGroupMember();
        }
        // perform the invite
        if (member.getState().equals(TalkGroupMember.STATE_NONE)) {
            // set up the member
            member.setGroupId(groupId);
            member.setClientId(clientId);
            member.setState(TalkGroupMember.STATE_INVITED);
            member.trashPrivate();
            changedGroupMember(member, new Date(), true);
            //  NOTE if this gets removed then the invited users presence might
            //       need touching depending on what the solution to the update problem is
            // notify various things
            touchGroupMemberPresences(groupId);
            mServer.getUpdateAgent().requestGroupUpdate(groupId, clientId);
            mServer.getUpdateAgent().requestGroupMembershipUpdatesForNewMember(groupId, clientId);
            mServer.getUpdateAgent().requestPresenceUpdateForGroup(clientId, groupId);
            mServer.getUpdateAgent().requestPresenceUpdate(clientId, null);
        } else {
            throw new RuntimeException("Already invited or member to group");
        }
    }

    @Override
    public void joinGroup(String groupId) {
        requireIdentification();
        logCall("joinGroup(groupId: '" + groupId + "')");

        String clientId = mConnection.getClientId();

        TalkGroupMember member = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (member == null) {
            throw new RuntimeException("Group does not exist");
        }

        if (member.getState().equals(TalkGroupMember.STATE_JOINED)) {
            throw new RuntimeException("Already a member of the group");
        }

        if (!member.getState().equals(TalkGroupMember.STATE_INVITED)) {
            throw new RuntimeException("Not invited to group");
        }

        member.setState(TalkGroupMember.STATE_JOINED);

        changedGroupMember(member, new Date(), false);
    }

    @Override
    public void leaveGroup(String groupId) {
        requireIdentification();
        TalkGroupMember member = requiredGroupInvitedOrMember(groupId);
        logCall("leaveGroup(groupId: '" + groupId + "')");
        // set membership state to NONE
        member.setState(TalkGroupMember.STATE_NONE);
        // degrade anyone who leaves to member
        member.setRole(TalkGroupMember.ROLE_MEMBER);
        // trash keys
        member.trashPrivate();
        // save the whole thing
        changedGroupMember(member, new Date(), false);
    }

    @Override
    public void removeGroupMember(String groupId, String clientId) {
        requireIdentification();
        requireGroupAdmin(groupId);
        //requireNotNearbyGroupType(groupId);
        logCall("removeGroupMember(groupId: '" + groupId + "' / clientId: '" + clientId + "')");

        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        // set membership state to NONE
        targetMember.setState(TalkGroupMember.STATE_NONE);
        // degrade removed users to member
        targetMember.setRole(TalkGroupMember.ROLE_MEMBER);
        targetMember.trashPrivate();
        changedGroupMember(targetMember, new Date(), false);
    }

    @Override
    public void updateGroupRole(String groupId, String clientId, String role) {
        requireIdentification();
        requireGroupAdmin(groupId);
        logCall("updateGroupRole(groupId: '" + groupId + "' / clientId: '" + clientId + "', role: '" + role + "')");
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        if (!TalkGroupMember.isValidRole(role)) {
            throw new RuntimeException("Invalid role");
        }
        targetMember.setRole(role);
        changedGroupMember(targetMember, new Date(), false);
    }

    @Override
    public TalkGroupMember[] getGroupMembers(String groupId, Date lastKnown) {
        requireIdentification();
        requiredGroupInvitedOrMember(groupId);
        logCall("getGroupMembers(groupId: '" + groupId + "' / lastKnown: '" + lastKnown + "')");

        List<TalkGroupMember> members = mDatabase.findGroupMembersByIdChangedAfter(groupId, lastKnown);
        TalkGroupMember[] res = new TalkGroupMember[members.size()];
        for (int i = 0; i < res.length; i++) {
            TalkGroupMember m = members.get(i);
            if (!m.getClientId().equals(mConnection.getClientId())) {
                m.setEncryptedGroupKey(null);
            }
            res[i] = m;
        }
        return res;
    }

    private void changedGroup(TalkGroup group, Date changed) {
        group.setLastChanged(changed);
        mDatabase.saveGroup(group);
        mServer.getUpdateAgent().requestGroupUpdate(group.getGroupId());
    }

    private void changedGroupMember(TalkGroupMember member, Date changed, boolean isNewMember) {
        member.setLastChanged(changed);
        mDatabase.saveGroupMember(member);
        mServer.getUpdateAgent().requestGroupMembershipUpdate(member.getGroupId(), member.getClientId(), isNewMember);
    }

    private void requireGroupAdmin(String groupId) {
        TalkGroupMember gm = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if (gm != null && gm.isAdmin()) {
            return;
        }
        throw new RuntimeException("Client is not an admin in group with id: '" + groupId + "'");
    }

    private TalkGroupMember requiredGroupInvitedOrMember(String groupId) {
        TalkGroupMember gm = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if (gm != null && (gm.isInvited() || gm.isMember())) {
            return gm;
        }
        throw new RuntimeException("Client is not a member in group with id: '" + groupId + "'");
    }
/*
    private void requireNotNearbyGroupType(String groupId) {
        // perspectively we should evolve a permission model to enable checking of WHO is allowed to do WHAT in which CONTEXT
        // e.g. client (permission depending on role) inviteGroupMembers to Group (permission depending on type)
        // for now we just check for nearby groups...
        TalkGroup group = mDatabase.findGroupById(groupId);
        if (group.isTypeNearby()) {
            throw new RuntimeException("Group type is: nearby. not allowed.");
        }
    }
*/
    @Override
    public FileHandles createFileForStorage(int contentLength) {
        requireIdentification();
        logCall("createFileForStorage(contentLength: '" + contentLength + "')");
        return mServer.getFilecacheClient()
                .createFileForStorage(mConnection.getClientId(), "application/octet-stream", contentLength);
    }

    @Override
    public FileHandles createFileForTransfer(int contentLength) {
        requireIdentification();
        logCall("createFileForTransfer(contentLength: '" + contentLength + "')");
        return mServer.getFilecacheClient()
                .createFileForTransfer(mConnection.getClientId(), "application/octet-stream", contentLength);
    }

    // should be called by the receiver of an transfer file if the user has aborted the download
    @Override
    public String receivedFile(String fileId) {
        requireIdentification();
        logCall("receivedFile(fileId: '" + fileId + "')");
        return processFileDownloadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_RECEIVED);
    }
    // should be called by the receiver of an transfer file after download; the server can the delete the file in case
    @Override
    public String abortedFileDownload(String fileId) {
        requireIdentification();
        logCall("abortedFileDownload(fileId: '" + fileId + "')");
        return processFileDownloadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_ABORTED);
    }
    // should be called by the receiver of an transfer file if the client has exceeded the download retry count
    @Override
    public String failedFileDownload(String fileId) {
        requireIdentification();
        logCall("failedFileDownload(fileId: '" + fileId + "')");
        return processFileDownloadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_FAILED);
    }
    // should be called by the receiver of an transfer file when a final attachment sender set state has been seen
    @Override
    public String acknowledgeAbortedFileUpload(String fileId) {
        requireIdentification();
        logCall("failedFileDownload(fileId: '" + fileId + "')");
        return processFileDownloadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOAD_ABORTED_ACKNOWLEDGED);
    }
    // should be called by the receiver of an transfer file when a final attachment sender set state has been seen
    @Override
    public String acknowledgeFailedFileUpload(String fileId) {
        requireIdentification();
        logCall("acknowledgeFailedFileUpload(fileId: '" + fileId + "')");
        return processFileDownloadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOAD_FAILED_ACKNOWLEDGED);
    }

    private String processFileDownloadMessage(String fileId, String nextState) {
        final String clientId = mConnection.getClientId();
        logCall("processFileDownloadMessage(fileId: '" + fileId + "') for client "+clientId + ", nextState='"+nextState+"'");

        List<TalkMessage> messages = mDatabase.findMessagesWithAttachmentFileId(fileId);
        if (messages.size() == 0) {
            throw new RuntimeException("No message found with file id "+fileId);
        }
        if (messages.size() > 1) {
            LOG.error("Multiple messages ("+messages.size()+") found with file id " + fileId);
        }
        for (TalkMessage message : messages) {
            if (clientId.equals(message.getSenderId())) {
                throw new RuntimeException("Sender must not mess with download, messageId="+message.getMessageId());
            }
            synchronized (mServer.idLock(message.getMessageId())) {
                TalkDelivery delivery = mDatabase.findDelivery(message.getMessageId(), clientId);
                if (delivery != null) {
                    LOG.info("AttachmentState '"+delivery.getAttachmentState()+"' --> '"+nextState+"' (download), messageId="+message.getMessageId()+", delivery="+delivery.getId());
                    if (!delivery.nextAttachmentStateAllowed(nextState)) {
                        throw new RuntimeException("next state '"+nextState+"'not allowed, delivery already in state '"+delivery.getAttachmentState()+"', messageId="+message.getMessageId()+", delivery="+delivery.getId());
                    }
                    delivery.setAttachmentState(nextState);
                    delivery.setTimeChanged(new Date());
                    mDatabase.saveDelivery(delivery);
                    mServer.getDeliveryAgent().requestDelivery(delivery.getSenderId(), false);
                } else {
                    throw new RuntimeException("delivery not found, messageId="+message.getMessageId());
                }
            }
        }
        return nextState;
    }

    //------ sender attachment state indication methods
    // should be called by the sender of an transfer file after upload has been started
    @Override
    public String startedFileUpload(String fileId) {
        requireIdentification();
        logCall("startedFileUpload(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOADING, null);
    }
    // should be called by the sender of an transfer file when the upload has been paused
    @Override
    public String pausedFileUpload(String fileId) {
        requireIdentification();
        logCall("pausedFileUpload(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOAD_PAUSED, null);
    }
    // should be called by the sender of an transfer file after upload has been finished
    @Override
    public String finishedFileUpload(String fileId) {
        requireIdentification();
        logCall("finishedFileUpload(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOADED, null);
    }
    // should be called by the sender of an transfer file when the upload is aborted by the user
    @Override
    public String abortedFileUpload(String fileId) {
        requireIdentification();
        logCall("abortedFileUpload(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOAD_ABORTED, null);
    }
    // should be called by the sender of an transfer file when upload retry count has been exceeded
    @Override
    public String failedFileUpload(String fileId) {
        requireIdentification();
        logCall("failedFileUpload(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_UPLOAD_FAILED, null);
    }
    // should be called by the sender of an transfer file when a final attachment receiver set state has been seen
    @Override
    public String acknowledgeReceivedFile(String fileId, String receiverId) {
        requireIdentification();
        logCall("acknowledgeReceivedFile(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_RECEIVED_ACKNOWLEDGED, receiverId);
    }
    // should be called by the sender of an transfer file when a final attachment receiver set state has been seen
    @Override
    public String acknowledgeAbortedFileDownload(String fileId, String receiverId) {
        requireIdentification();
        logCall("acknowledgeReceivedFile(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_ABORTED_ACKNOWLEDGED, receiverId);
    }
    // should be called by the sender of an transfer file when a final attachment receiver set state has been seen
    @Override
    public String acknowledgeFailedFileDownload(String fileId, String receiverId) {
        requireIdentification();
        logCall("acknowledgeReceivedFile(fileId: '" + fileId + "')");
        return processFileUploadMessage(fileId, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_FAILED_ACKNOWLEDGED, receiverId);
    }

    private String processFileUploadMessage(String fileId, String nextState, String receiverId) {
        final String clientId = mConnection.getClientId();

        List<TalkMessage> messages = mDatabase.findMessagesWithAttachmentFileId(fileId);
        if (messages.size() == 0) {
            throw new RuntimeException("No message found with file id "+fileId);
        }
        for (TalkMessage message : messages) {
            if (clientId.equals(message.getSenderId())) {
                synchronized (mServer.idLock(message.getMessageId())) {
                    message.setAttachmentUploadStarted(new Date());
                    mDatabase.saveMessage(message);
                    List<TalkDelivery> deliveries = mDatabase.findDeliveriesForMessage(message.getMessageId());
                    for (TalkDelivery delivery : deliveries) {
                        // for some calls we update only a specific delivery, while for other calls we update only the delivery with the proper receiverId
                        if (receiverId == null || delivery.getReceiverId().equals(receiverId)) {
                            LOG.info("AttachmentState '"+delivery.getAttachmentState()+"' --> '"+nextState+"' (upload), messageId="+message.getMessageId()+", delivery="+delivery.getId());
                            if (!delivery.nextAttachmentStateAllowed(nextState)) {
                                throw new RuntimeException("next state '"+nextState+"'not allowed, delivery already in state '"+delivery.getAttachmentState()+"', messageId="+message.getMessageId()+", delivery="+delivery.getId());
                            }
                            delivery.setAttachmentState(nextState);
                            delivery.setTimeChanged(message.getAttachmentUploadStarted());
                            mDatabase.saveDelivery(delivery);
                            mServer.getDeliveryAgent().requestDelivery(delivery.getReceiverId(), false);
                        }
                    }
                }
            } else {
                throw new RuntimeException("you are not the sender of this file with messageId="+message.getMessageId());
            }
        }
        return nextState;
    }


    // should be called by the sender of an transfer file after upload has been finished
    //void finishedFileUpload(String fileId);

    /*
    @Override
    public void deleteFile(String fileId) {
        requireIdentification();
        logCall("deleteFile(fileId: '" + fileId + "')");
        mServer.getFilecacheClient().deleteFile(fileId);
        // TODO: notify receivers
    }
    */
    private void createGroupWithEnvironment(TalkEnvironment environment) {
        LOG.info("createGroupWithEnvironment: creating new group for client with id '" + mConnection.getClientId() + "'");
        TalkGroup group = new TalkGroup();
        group.setGroupTag(UUID.randomUUID().toString());
        group.setGroupId(UUID.randomUUID().toString());
        group.setState(TalkGroup.STATE_EXISTS);
        if (environment.getName() == null) {
            group.setGroupName(environment.getType() + "-" + group.getGroupId().substring(group.getGroupId().length() - 8));
        } else {
            group.setGroupName(environment.getName());
        }
        group.setGroupType(environment.getType());
        LOG.info("updateEnvironment: creating new group for client with id '" + mConnection.getClientId() + "' with type " + environment.getType());
        TalkGroupMember groupAdmin = new TalkGroupMember();
        groupAdmin.setClientId(mConnection.getClientId());
        groupAdmin.setGroupId(group.getGroupId());
        groupAdmin.setRole(TalkGroupMember.ROLE_NEARBY_MEMBER);
        groupAdmin.setState(TalkGroupMember.STATE_JOINED);
        changedGroup(group, new Date());
        changedGroupMember(groupAdmin, group.getLastChanged(), true);

        environment.setGroupId(group.getGroupId());
        environment.setClientId(mConnection.getClientId());
        mDatabase.saveEnvironment(environment);

        String currentGroupId = environment.getGroupId();
        String potentiallyOtherGroupId = updateEnvironment(environment);
        if (!currentGroupId.equals(potentiallyOtherGroupId)) {
            LOG.info("createGroupWithEnvironment Collision detected: determined there is actually another group we were merged with...");
            LOG.info("  * original groupId: '" + currentGroupId + "' - new groupId: '" + potentiallyOtherGroupId + "'");
            environment.setGroupId(potentiallyOtherGroupId);

            // Now perform a hard-delete of old group - notifications have not yet been sent out, so this is ok
            mDatabase.deleteGroup(group);
        }
    }

    private void joinGroupWithEnvironment(TalkGroup group, TalkEnvironment environment) {
        LOG.info("joinGroupWithEnvironment: joining group with client id '" + mConnection.getClientId() + "'");

        TalkGroupMember nearbyMember = mDatabase.findGroupMemberForClient(group.getGroupId(), mConnection.getClientId());
        boolean isNew = false;
        if (nearbyMember == null) {
            nearbyMember = new TalkGroupMember();
            isNew = true;
        }
        nearbyMember.setClientId(mConnection.getClientId());
        nearbyMember.setGroupId(group.getGroupId());
        nearbyMember.setRole(TalkGroupMember.ROLE_NEARBY_MEMBER);
        // TODO: Idea: if we would only invite here the client would only receive nearby group messages after joining, which
        // the clients could do on their discretion
        nearbyMember.setState(TalkGroupMember.STATE_JOINED);
        if (!group.getState().equals(TalkGroup.STATE_EXISTS)) {
            group.setState(TalkGroup.STATE_EXISTS);
            mDatabase.saveGroup(group);
        }
        changedGroup(group, new Date());
        changedGroupMember(nearbyMember, group.getLastChanged(), isNew);

        environment.setGroupId(group.getGroupId());
        environment.setClientId(mConnection.getClientId());
        mDatabase.saveEnvironment(environment);

        touchGroupMemberPresences(group.getGroupId());
        mServer.getUpdateAgent().requestGroupUpdate(group.getGroupId(), mConnection.getClientId());
        mServer.getUpdateAgent().requestGroupMembershipUpdatesForNewMember(group.getGroupId(), mConnection.getClientId());
        mServer.getUpdateAgent().requestPresenceUpdateForGroup(mConnection.getClientId(), group.getGroupId());
        mServer.getUpdateAgent().requestPresenceUpdate(mConnection.getClientId(), null);
    }

    public ArrayList<Pair<String, Integer>> findGroupSortedBySize(List<TalkEnvironment> matchingEnvironments) {
        Map<String, Integer> environmentsPerGroup = new HashMap<String, Integer>();
        for (int i = 0; i < matchingEnvironments.size(); ++i) {
            String key = matchingEnvironments.get(i).getGroupId();
            if (environmentsPerGroup.containsKey(key)) {
                environmentsPerGroup.put(key, environmentsPerGroup.get(key) + 1);
            } else {
                environmentsPerGroup.put(key, 1);
            }
        }
        environmentsPerGroup = MapUtil.sortByValueDescending(environmentsPerGroup);

        ArrayList<Pair<String, Integer>> result = new ArrayList<Pair<String, Integer>>();
        for (Map.Entry<String, Integer> entry : environmentsPerGroup.entrySet()) {
            result.add(new ImmutablePair<String, Integer>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    @Override
    public String updateEnvironment(TalkEnvironment environment) {
        logCall("updateEnvironment(clientId: '" + mConnection.getClientId() + "')");
        requireIdentification();

        if (environment.getType() == null) {
            LOG.warn("updateEnvironment: no environment type, defaulting to nearby. Please fix client");
            environment.setType(TalkEnvironment.TYPE_NEARBY);
        }

        environment.setTimeReceived(new Date());
        environment.setClientId(mConnection.getClientId());

        List<TalkEnvironment> matching = mDatabase.findEnvironmentsMatching(environment);
        TalkEnvironment myEnvironment = mDatabase.findEnvironmentByClientId(environment.getType(), mConnection.getClientId());
        ArrayList<Pair<String, Integer>> environmentsPerGroup = findGroupSortedBySize(matching);

        for (TalkEnvironment te : matching) {
            if (te.getClientId().equals(mConnection.getClientId())) {
                // there is already a matching environment for us
                TalkGroupMember myMemberShip = mDatabase.findGroupMemberForClient(te.getGroupId(), te.getClientId());
                TalkGroup myGroup = mDatabase.findGroupById(te.getGroupId());
                if (myMemberShip != null && myGroup != null) {
                    if (myMemberShip.isAdmin() && myMemberShip.isJoined() && myGroup.getState().equals(TalkGroup.STATE_EXISTS)) {
                        // everything seems fine, but are we in the largest group?
                        if (environmentsPerGroup.size() > 1) {
                            if (!environmentsPerGroup.get(0).getLeft().equals(te.getGroupId())) {
                                // we are not in the largest group, lets move over
                                destroyEnvironment(myEnvironment);
                                // join the largest group
                                TalkGroup largestGroup = mDatabase.findGroupById(environmentsPerGroup.get(0).getLeft());
                                joinGroupWithEnvironment(largestGroup, environment);
                                return largestGroup.getGroupId();
                            }
                        }
                        // group membership has not changed, we are still fine
                        // just update environment
                        if (environment.getGroupId() == null) {
                            // first environment update without a group known to the client, but group exists on server, probably unclean connection shutdown
                            environment.setGroupId(myEnvironment.getGroupId());
                        } else if (!environment.getGroupId().equals(myEnvironment.getGroupId())) {
                            // matching environment found, but group id differs from old environment, which must not happen - client wants to gain membership to a group it is not entitled to
                            // lets destroy all environments
                            destroyEnvironment(te);
                            destroyEnvironment(myEnvironment);
                            throw new RuntimeException("illegal group id in environment");
                        }
                        myEnvironment.updateWith(environment);
                        mDatabase.saveEnvironment(myEnvironment);
                        return myGroup.getGroupId();
                    }
                    // there is a group and a membership, but they seem to be tombstones, so lets ignore them, just get rid of the bad environment
                    mDatabase.deleteEnvironment(te);
                    TalkGroup largestGroup = mDatabase.findGroupById(environmentsPerGroup.get(0).getLeft());
                    joinGroupWithEnvironment(largestGroup, environment);
                    return largestGroup.getGroupId();
                }
            }
        }
        // when we got here, there is no environment for us in the matching list
        if (myEnvironment != null) {
            // we have an environment for another location that does not match, lets get rid of it
            destroyEnvironment(myEnvironment);
        }
        if (!matching.isEmpty()) {
            // join the largest group
            TalkGroup largestGroup = mDatabase.findGroupById(environmentsPerGroup.get(0).getLeft());
            joinGroupWithEnvironment(largestGroup, environment);
            return largestGroup.getGroupId();
        }
        // we are alone or first at the location, lets create a new group
        createGroupWithEnvironment(environment);
        return environment.getGroupId();
    }

    private void removeGroupMember(TalkGroupMember member, Date now) {
        // remove my membership
        // set membership state to NONE
        member.setState(TalkGroupMember.STATE_NONE);
        // degrade removed users to member
        member.setRole(TalkGroupMember.ROLE_MEMBER);
        member.trashPrivate();
        changedGroupMember(member, now, false);
    }

    private void destroyEnvironment(TalkEnvironment environment) {
        logCall("destroyEnvironment(" + environment + ")");
        TalkGroup group = mDatabase.findGroupById(environment.getGroupId());
        TalkGroupMember member = mDatabase.findGroupMemberForClient(environment.getGroupId(), environment.getClientId());
        if (member != null && member.getState().equals(TalkGroupMember.STATE_JOINED)) {
            Date now = new Date();
            removeGroupMember(member, now);
            String[] states = {TalkGroupMember.STATE_JOINED};
            List<TalkGroupMember> membersLeft = mDatabase.findGroupMembersByIdWithStates(environment.getGroupId(), states);
            logCall("destroyEnvironment: membersLeft: " + membersLeft.size());

            // clean up other offline members that somehow might be stuck in the group
            // although this should never happen except on crash or server restart
            // The canonical place would be to check this on group join, but here
            // we already have a list of all remaining members, so it will be faster
            // and should cause less trouble than doing it on joining
            int removedCount = 0;
            for (int i = 0; i < membersLeft.size(); ++i) {
                // cleanup other offline members
                TalkGroupMember otherMember = membersLeft.get(i);
                boolean isConnected = mServer.isClientConnected(otherMember.getClientId());
                if (!isConnected) {
                    // remove offline member from group
                    removeGroupMember(otherMember, now);
                    ++removedCount;
                }
            }
            logCall("destroyEnvironment: offline members removed: " + removedCount);

            if (membersLeft.size() - removedCount <= 0) {
                logCall("destroyEnvironment: last member left, removing group " + group.getGroupId());
                // last member removed, remove group
                group.setState(TalkGroup.STATE_NONE);
                changedGroup(group, now);
                // explicitly request a group updated notification for the last removed client because
                // calling changedGroup only will not send out "groupUpdated" notifications to members with state "none"
                mServer.getUpdateAgent().requestGroupUpdate(group.getGroupId(), environment.getClientId());
            }
        }
        mDatabase.deleteEnvironment(environment);
    }

    @Override
    public void destroyEnvironment(String type) {
        requirePastIdentification();
        logCall("destroyEnvironment(clientId: '" + mConnection.getClientId() + "')");

        if (type == null) {
            LOG.warn("destroyEnvironment: no environment type, defaulting to nearby. Please fix client");
            type = TalkEnvironment.TYPE_NEARBY;
        }

        TalkEnvironment myEnvironment;
        while ((myEnvironment = mDatabase.findEnvironmentByClientId(type, mConnection.getClientId())) != null) {
            destroyEnvironment(myEnvironment);
        }
    }

    @Override
    public Boolean[] isMemberInGroups(String[] groupIds) {
        requireIdentification();
        ArrayList<Boolean> result = new ArrayList<Boolean>();
        logCall("isMemberInGroups(groupIds: '" + Arrays.toString(groupIds) + "'");
        String clientId = mConnection.getClientId();

        for (String groupId : groupIds) {
            TalkGroupMember membership = mDatabase.findGroupMemberForClient(groupId, clientId);
            if (membership != null && (membership.isInvited() || membership.isMember())) {
                result.add(true);
            } else {
                result.add(false);
            }
        }

        return result.toArray(new Boolean[result.size()]);
    }

    // return true if for each client the caller is related to by a relationsShip or by an active group membership
    @Override
    public Boolean[] isContactOf(String[] clientIds) {
        requireIdentification();
        logCall("isContactOf(clientIds: '" + Arrays.toString(clientIds) + "'");
        final String clientId = mConnection.getClientId();

        final List<TalkRelationship> relationships =
                mDatabase.findRelationshipsForClientInStates(clientId, TalkRelationship.STATES_RELATED);

        Set<String> myContactIds = new HashSet<String>();
        for (TalkRelationship relationship : relationships) {
            myContactIds.add(relationship.getOtherClientId());
        }

        final List<TalkGroupMember> myMemberships = mDatabase.findGroupMembersForClientWithStates(clientId, TalkGroupMember.ACTIVE_STATES);

        for (TalkGroupMember groupMember : myMemberships) {
            final List<TalkGroupMember> myGroupContacts = mDatabase.findGroupMembersByIdWithStates(groupMember.getGroupId(),TalkGroupMember.ACTIVE_STATES);
            for (TalkGroupMember member : myGroupContacts) {
                myContactIds.add(member.getClientId());
            }
        }

        ArrayList<Boolean> result = new ArrayList<Boolean>();

        for (String contactId : clientIds) {
            if (myContactIds.contains(contactId)) {
                 result.add(true);
            } else {
                result.add(false);
            }
        }

        return result.toArray(new Boolean[result.size()]);
    }
}
