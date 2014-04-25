package com.hoccer.talk.server.rpc;

import com.hoccer.talk.model.*;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.ITalkServerStatistics;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
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

    private static Hex HEX = new Hex();
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
    private TalkServer mServer;

    /**
     * Reference to database accessor
     */
    private ITalkServerDatabase mDatabase;

    /**
     * Reference to stats collector
     */
    private ITalkServerStatistics mStatistics;

    /**
     * Reference to connection object
     */
    private TalkRpcConnection mConnection;

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

    private void requireIdentification() {
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
    public TalkServerInfo hello(TalkClientInfo clientInfo) {
        logCall("hello()");

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

        TalkServerInfo serverInfo = new TalkServerInfo();
        serverInfo.setServerTime(new Date());
        serverInfo.setSupportMode(mConnection.isSupportMode());

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

        // XXX check verifier and salt for viability

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
            throw new RuntimeException("Can't authenticate while logged in");
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
                throw new RuntimeException("No such client");
            }

            // verify SRP registration
            if (mSrpClient.getSrpVerifier() == null || mSrpClient.getSrpSalt() == null) {
                throw new RuntimeException("No such client");
            }

            // parse the salt from DB
            byte[] salt = null;
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
            byte[] M1b = null;
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
        if (registrationToken.length() == 0) {
            return;
        }
        // set and save the token
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

        logCall("getRelationships(lastKnown: '" + lastKnown.toString() + "')");

        // query the db
        List<TalkRelationship> relationships =
                mDatabase.findRelationshipsChangedAfter(mConnection.getClientId(), lastKnown);

        // build the result array
        TalkRelationship[] res = new TalkRelationship[relationships.size()];
        int idx = 0;
        for (TalkRelationship r : relationships) {
            res[idx++] = r;
        }

        // return the bunch
        return res;
    }

    @Override
    public void updatePresence(TalkPresence presence) {
        requireIdentification();

        logCall("updatePresence()");

        // find existing presence or create one
        TalkPresence existing = mDatabase.findPresenceForClient(mConnection.getClientId());
        if (existing == null) {
            existing = new TalkPresence();
        }

        // update the presence with what we got
        existing.setClientId(mConnection.getClientId());
        existing.setClientName(presence.getClientName());
        existing.setClientStatus(presence.getClientStatus());
        existing.setTimestamp(new Date());
        existing.setAvatarUrl(presence.getAvatarUrl());
        existing.setKeyId(presence.getKeyId());

        // save the thing
        mDatabase.savePresence(existing);

        // start updating other clients
        mServer.getUpdateAgent().requestPresenceUpdate(mConnection.getClientId());
    }

    @Override
    public TalkPresence[] getPresences(Date lastKnown) {
        requireIdentification();

        logCall("getPresences(lastKnown: '" + lastKnown + "')");

        // perform the query
        List<TalkPresence> pres = mDatabase.findPresencesChangedAfter(mConnection.getClientId(), lastKnown);

        // update connection status and convert results to array
        TalkPresence[] res = new TalkPresence[pres.size()];
        for (int i = 0; i < res.length; i++) {
            TalkPresence p = pres.get(i);
            p.setConnectionStatus(mServer.isClientConnected(p.getClientId())
                    ? TalkPresence.CONN_STATUS_ONLINE : TalkPresence.CONN_STATUS_OFFLINE);
            res[i] = pres.get(i);
        }

        // return it
        return res;
    }

    @Override
    public void updateKey(TalkKey key) {
        requireIdentification();

        logCall("updateKey()");

        TalkKey k = mDatabase.findKey(mConnection.getClientId(), key.getKeyId());
        if (k != null) {
            return;
        }

        key.setClientId(mConnection.getClientId());
        key.setTimestamp(new Date());

        // XXX should check if the content is ok

        mDatabase.saveKey(key);
    }

    @Override
    public TalkKey getKey(String clientId, String keyId) {
        requireIdentification();

        logCall("getKey(clientId: '" + clientId + "', keyId: '" + keyId + "')");

        TalkKey res = null;

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if (rel != null && rel.isFriend()) {
            res = mDatabase.findKey(clientId, keyId);
        } else {
            List<TalkGroupMember> members = mDatabase.findGroupMembersForClient(mConnection.getClientId());
            for (TalkGroupMember member : members) {
                if (member.isJoined() || member.isInvited()) {
                    TalkGroupMember otherMember = mDatabase.findGroupMemberForClient(member.getGroupId(), clientId);
                    if (otherMember != null && (otherMember.isJoined() || otherMember.isInvited())) {
                        res = mDatabase.findKey(clientId, keyId);
                        break;
                    }
                }
            }
        }

        if (res == null) {
            throw new RuntimeException("Given client is not your friend or key does not exist");
        }

        return res;
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

        // log about it
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

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if (rel == null) {
            throw new RuntimeException("You are not paired with client with id '" + clientId + "'");
        }

        String oldState = rel.getState();

        if (oldState.equals(TalkRelationship.STATE_FRIEND)) {
            setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_BLOCKED, true);
            return;
        }
        if (oldState.equals(TalkRelationship.STATE_BLOCKED)) {
            return;
        }

        throw new RuntimeException("You are not paired with client with id '" + clientId + "'");
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

        if (oldState.equals(TalkRelationship.STATE_FRIEND)) {
            return;
        }
        if (oldState.equals(TalkRelationship.STATE_BLOCKED)) {
            setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_FRIEND, true);
            return;
        }

        throw new RuntimeException("You are not paired with client with id '" + clientId + "'");
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
        LOG.info("relationship between clients with id '" + thisClientId + "' and '" + otherClientId + "' is now in state '" + state + "'");
        TalkRelationship relationship = mDatabase.findRelationshipBetween(thisClientId, otherClientId);
        if (relationship == null) {
            relationship = new TalkRelationship();
        }
        relationship.setClientId(thisClientId);
        relationship.setOtherClientId(otherClientId);
        relationship.setState(state);
        relationship.setLastChanged(new Date());
        mDatabase.saveRelationship(relationship);
        if (notify) {
            mServer.getUpdateAgent().requestRelationshipUpdate(relationship);
        }
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();

        logCall("deliveryRequest(" + deliveries.length + " deliveries)");

        // who is doing this again?
        String clientId = mConnection.getClientId();

        // generate and assign message id
        String messageId = UUID.randomUUID().toString();
        message.setMessageId(messageId);

        // guarantee correct sender
        message.setSenderId(clientId);

        // walk deliveries and determine which to accept,
        // filling in missing things as we go
        Vector<TalkDelivery> acceptedDeliveries = new Vector<TalkDelivery>();
        for (TalkDelivery d : deliveries) {
            // fill out various fields
            d.setMessageId(message.getMessageId());
            d.setSenderId(clientId);
            // perform the delivery request
            acceptedDeliveries.addAll(requestOneDelivery(message, d));
        }

        // update number of deliveries
        message.setNumDeliveries(acceptedDeliveries.size());

        // process all accepted deliveries
        if (!acceptedDeliveries.isEmpty()) {
            // save deliveries first so messages get collected
            for (TalkDelivery ds : acceptedDeliveries) {
                mDatabase.saveDelivery(ds);
            }
            // save the message
            mDatabase.saveMessage(message);
            // initiate delivery for all recipients
            for (TalkDelivery ds : acceptedDeliveries) {
                mServer.getDeliveryAgent().requestDelivery(ds.getReceiverId());
            }
        }

        mStatistics.signalMessageAcceptedSucceeded();

        // done - return whatever we are left with
        return deliveries;
    }

    private Vector<TalkDelivery> requestOneDelivery(TalkMessage message, TalkDelivery delivery) {
        Vector<TalkDelivery> result = new Vector<TalkDelivery>();

        // get the current date for stamping
        Date currentDate = new Date();
        // who is doing this again?
        String senderId = mConnection.getClientId();
        // get the receiver
        String recipientId = delivery.getReceiverId();

        // if we don't have a receiver try group delivery
        if (recipientId == null) {
            String groupId = delivery.getGroupId();

            if (groupId == null) {
                LOG.info("delivery rejected: no receiver");
                delivery.setState(TalkDelivery.STATE_FAILED);
                return result;
            }
            // check that group exists
            TalkGroup group = mDatabase.findGroupById(groupId);
            if (group == null) {
                LOG.info("delivery rejected: no such group");
                delivery.setState(TalkDelivery.STATE_FAILED);
                return result;
            }
            // check that sender is member of group
            TalkGroupMember clientMember = mDatabase.findGroupMemberForClient(groupId, senderId);
            if (clientMember == null || !clientMember.isMember()) {
                LOG.info("delivery rejected: not a member of group");
                delivery.setState(TalkDelivery.STATE_FAILED);
                return result;
            }
            // deliver to each group member
            List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
            for (TalkGroupMember member : members) {
                if (member.getClientId().equals(senderId)) {
                    continue;
                }
                if (!member.isJoined()) {
                    continue;
                }
                if (member.getEncryptedGroupKey() == null) {
                    LOG.warn("have no group key, discarding group message "+message.getMessageId()+" for client "+member.getClientId()+" group "+groupId);
                    continue;
                }
                if (member.getSharedKeyId() != null && message.getSharedKeyId() != null && !member.getSharedKeyId().equals(message.getSharedKeyId())) {
                    LOG.warn("message key id and member shared key id mismatch, discarding group message "+message.getMessageId()+" for client "+member.getClientId()+" group "+groupId);
                    LOG.warn("message.sharedKeyId="+message.getSharedKeyId()+" member.sharedKeyId= "+member.getSharedKeyId() +" group.sharedKeyId= "+group.getSharedKeyId());
                    continue;
                }
                LOG.info("delivering message " + message.getMessageId() + " for client " + member.getClientId() + " group " + groupId + " sharedKeyId=" + message.getSharedKeyId() + ", member sharedKeyId=" + member.getSharedKeyId());

                TalkDelivery memberDelivery = new TalkDelivery();
                memberDelivery.setMessageId(message.getMessageId());
                memberDelivery.setMessageTag(delivery.getMessageTag());
                memberDelivery.setGroupId(groupId);
                memberDelivery.setSenderId(senderId);
                memberDelivery.setKeyId(member.getMemberKeyId());
                memberDelivery.setKeyCiphertext(member.getEncryptedGroupKey());
                memberDelivery.setReceiverId(member.getClientId());
                memberDelivery.setState(TalkDelivery.STATE_DELIVERING);
                memberDelivery.setTimeAccepted(currentDate);

                boolean success = performOneDelivery(message, memberDelivery);
                if (success) {
                    result.add(memberDelivery);
                    // group deliveries are confirmed from acceptance
                    delivery.setState(TalkDelivery.STATE_CONFIRMED);
                    // set delivery timestamps
                    delivery.setTimeAccepted(currentDate);
                    delivery.setTimeChanged(currentDate);
                }
            }
        } else {
            // Check if client is friend via relationship OR they know each other through a shared group in which both are "joined" members.
            if (areBefriended(senderId, recipientId) ||
                areRelatedViaGroupMembership(senderId, recipientId)) {
                if (performOneDelivery(message, delivery)) {
                    result.add(delivery);
                    // mark delivery as in progress
                    delivery.setState(TalkDelivery.STATE_DELIVERING);
                    // set delivery timestamps
                    delivery.setTimeAccepted(currentDate);
                    delivery.setTimeChanged(currentDate);
                }
            } else {
                LOG.info("Message delivery rejected since no relationship via group or friendship exists. (" + senderId + ", " + recipientId + ")");
                delivery.setState(TalkDelivery.STATE_FAILED);
            }
        }
        return result;
    }

    private boolean areRelatedViaGroupMembership(String clientId1, String clientId2) {
        // TODO: only allow this for joined members?!

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

    private boolean areBefriended(String clientId1, String clientId2) {
        final TalkRelationship relationship = mDatabase.findRelationshipBetween(clientId1, clientId2);

        // reject if there is no relationship
        if (relationship == null) {
            LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' have no relationship with each other");
            return false;
        }

        // reject unless befriended
        if (!relationship.getState().equals(TalkRelationship.STATE_FRIEND)) {
            LOG.info("clients '" + clientId1 + "' and '" + clientId2 +
                     "' are not friends (relationship is '" + relationship.getState() + "')");
            return false;
        }

        LOG.info("clients '" + clientId1 + "' and '" + clientId2 + "' are friends!");
        return true;
    }

    private boolean performOneDelivery(TalkMessage m, TalkDelivery d) {
        // get the current date for stamping
        Date currentDate = new Date();
        // who is doing this again?
        String clientId = mConnection.getClientId();
        // get the receiver
        String receiverId = d.getReceiverId();

        // reject messages to self
        if (receiverId.equals(clientId)) {
            LOG.info("delivery rejected: send to self");
            // mark delivery failed
            d.setState(TalkDelivery.STATE_FAILED);
            return false;
        }

        // reject messages to nonexisting clients
        //   XXX this check does not currently work because findClient() creates instances
        TalkClient receiver = mDatabase.findClientById(receiverId);
        if (receiver == null) {
            LOG.info("delivery rejected: recipient with id '" + receiverId + "' does not exist");
            // mark delivery failed
            d.setState(TalkDelivery.STATE_FAILED);
            return false;
        }

        // all fine, delivery accepted
        LOG.info("delivery accepted for recipient with id '" + receiverId + "'");
        // return
        return true;
    }

    @Override
    public TalkDelivery deliveryConfirm(String messageId) {
        requireIdentification();
        logCall("deliveryConfirm(messageId: '" + messageId + "')");
        String clientId = mConnection.getClientId();
        TalkDelivery d = mDatabase.findDelivery(messageId, clientId);
        if (d != null) {
            if (d.getState().equals(TalkDelivery.STATE_DELIVERING)) {
                LOG.info("confirmed message with id '" + messageId + "' for client with id '" + clientId + "'");
                setDeliveryState(d, TalkDelivery.STATE_DELIVERED);
                mStatistics.signalMessageConfirmedSucceeded();
            }
        }
        return d;
    }

    @Override
    public TalkDelivery deliveryAcknowledge(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAcknowledge(messageId: '" + messageId + "', recipientId: '" + recipientId + "')");
        TalkDelivery d = mDatabase.findDelivery(messageId, recipientId);
        if (d != null) {
            if (d.getState().equals(TalkDelivery.STATE_DELIVERED)) {
                LOG.info("acknowledged message with id '" + messageId + "' for recipient with id '" + recipientId + "'");
                setDeliveryState(d, TalkDelivery.STATE_CONFIRMED);
                mStatistics.signalMessageAcknowledgedSucceeded();
            }
        }
        return d;
    }

    // TODO: do not allow abortion of message that are already delivered or confirmed
    @Override
    public TalkDelivery deliveryAbort(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAbort(messageId: '" + messageId + "', recipientId: '" + recipientId + "'");
        String clientId = mConnection.getClientId();
        TalkDelivery delivery = mDatabase.findDelivery(messageId, recipientId);
        if (delivery != null) {
            if (recipientId.equals(clientId)) {
                // abort incoming delivery, regardless of sender
                setDeliveryState(delivery, TalkDelivery.STATE_ABORTED);
            } else {
                // abort outgoing delivery iff we are the actual sender
                if (delivery.getSenderId().equals(clientId)) {
                    setDeliveryState(delivery, TalkDelivery.STATE_ABORTED);
                }
            }
        }
        return delivery;
    }

    private void setDeliveryState(TalkDelivery delivery, String state) {
        delivery.setState(state);
        delivery.setTimeChanged(new Date());
        mDatabase.saveDelivery(delivery);
        if (state.equals(TalkDelivery.STATE_DELIVERED)) {
            mServer.getDeliveryAgent().requestDelivery(delivery.getSenderId());
        } else if (state.equals(TalkDelivery.STATE_DELIVERING)) {
            mServer.getDeliveryAgent().requestDelivery(delivery.getReceiverId());
        } else if (delivery.isFinished()) {
            mServer.getCleaningAgent().cleanFinishedDelivery(delivery);
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
        changedGroup(group,new Date());
        changedGroupMember(groupAdmin, group.getLastChanged());
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
        TalkGroup group = mDatabase.findGroupById(groupId);
        return group;
    }

    @Override
    public TalkGroupMember getGroupMember(String groupId, String clientId) {
        requireIdentification();
        logCall("getGroupMember(groupId: '" + groupId + ", clientId:"+clientId+"')");
        TalkGroupMember groupMember = mDatabase.findGroupMemberForClient(groupId,clientId);
        return groupMember;
    }

    @Override
    public void updateGroupName(String groupId, String name) {
        requireIdentification();
        requireGroupAdmin(groupId);
        requireNotNearbyGroupType(groupId);
        logCall("updateGroupName(groupId: '" + groupId + "', name: '" + name + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(groupId);
        targetGroup.setGroupName(name);
        changedGroup(targetGroup,new Date());
    }

    @Override
    public void updateGroupAvatar(String groupId, String avatarUrl) {
        requireIdentification();
        requireGroupAdmin(groupId);
        requireNotNearbyGroupType(groupId);
        logCall("updateGroupAvatar(groupId: '" + groupId + "', avatarUrl: '" + avatarUrl + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(groupId);
        targetGroup.setGroupAvatarUrl(avatarUrl);
        changedGroup(targetGroup,new Date());
    }

    @Override
    public void updateGroup(TalkGroup group) {
        requireIdentification();
        requireGroupAdmin(group.getGroupId());
        logCall("updateGroup(groupId: '" + group.getGroupId() + "')");
        TalkGroup targetGroup = mDatabase.findGroupById(group.getGroupId());
        targetGroup.setGroupName(group.getGroupName());
        targetGroup.setGroupAvatarUrl(group.getGroupAvatarUrl());
        changedGroup(targetGroup,new Date());
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
        changedGroup(group,new Date());

        // walk the group and make everyone have a "none" relationship to it
        List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
        for (TalkGroupMember member : members) {
            if (member.isInvited() || member.isJoined()) {
                member.setState(TalkGroupMember.STATE_GROUP_REMOVED);

                // TODO: check if degrade role of admins is advisable
                /*if (member.isAdmin()) {
                    member.setRole(TalkGroupMember.ROLE_MEMBER);
                }*/
                changedGroupMember(member, group.getLastChanged());
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
        requireNotNearbyGroupType(groupId);
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
            changedGroupMember(member, new Date());
            //  NOTE if this gets removed then the invited users presence might
            //       need touching depending on what the solution to the update problem is
            // notify various things
            touchGroupMemberPresences(groupId);
            mServer.getUpdateAgent().requestGroupUpdate(groupId, clientId);
            mServer.getUpdateAgent().requestGroupMembershipUpdatesForNewMember(groupId, clientId);
            mServer.getUpdateAgent().requestPresenceUpdateForGroup(clientId, groupId);
            mServer.getUpdateAgent().requestPresenceUpdate(clientId);
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

        if (!member.getState().equals(TalkGroupMember.STATE_INVITED)) {
            throw new RuntimeException("Not invited to group");
        }

        member.setState(TalkGroupMember.STATE_JOINED);

        changedGroupMember(member, new Date());
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
        // save the whole thing
        changedGroupMember(member, new Date());
    }

    @Override
    public void removeGroupMember(String groupId, String clientId) {
        requireIdentification();
        requireGroupAdmin(groupId);
        requireNotNearbyGroupType(groupId);
        logCall("removeGroupMember(groupId: '" + groupId + "' / clientId: '" + clientId + "')");

        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        // set membership state to NONE
        targetMember.setState(TalkGroupMember.STATE_NONE);
        // degrade removed users to member
        targetMember.setRole(TalkGroupMember.ROLE_MEMBER);
        changedGroupMember(targetMember, new Date());
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
        changedGroupMember(targetMember, new Date());
    }
    /*
    @Override
    public void updateGroupKey(String groupId, String clientId, String keyId, String key) {
        requireIdentification();
        logCall("updateGroupKey(groupId: '" + groupId + "' / clientId: '" + clientId + "', keyId: '" + keyId + "')");
        TalkGroupMember selfMember = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if (selfMember == null || !(selfMember.isAdmin() || (selfMember.isMember() && clientId.equals(mConnection.getClientId())))) {
            throw new RuntimeException("updateGroupKeys: insufficient permissions");
        }
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        targetMember.setMemberKeyId(keyId);
        targetMember.setEncryptedGroupKey(key);
        changedGroupMember(targetMember, new Date());
    }
    */

    @Override
    public void updateMyGroupKey(String groupId,String sharedKeyId, String sharedKeyIdSalt, String publicKeyId, String cryptedSharedKey) {
        requireIdentification();
        String clientId = mConnection.getClientId();
        logCall("updateMyGroupKey(groupId: '" + groupId + "' / clientId: '" + clientId + "', sharedKeyId: '" + sharedKeyId + "', sharedKeyIdSalt: '" + sharedKeyIdSalt  + "', publicKeyId: '" + publicKeyId + "')");
        TalkGroupMember selfMember = mDatabase.findGroupMemberForClient(groupId, clientId);
        if (selfMember == null || !(selfMember.isAdmin() || (selfMember.isMember()))) {
            throw new RuntimeException("updateMyGroupKey: not a member of the group");
        }
        TalkGroup group = mDatabase.findGroupById(groupId);
        if (sharedKeyId == null || group.getSharedKeyId() == null || !sharedKeyId.equals(group.getSharedKeyId()) ||
            sharedKeyIdSalt == null || group.getSharedKeyIdSalt() == null || !sharedKeyIdSalt.equals(group.getSharedKeyIdSalt()))
        {
            throw new RuntimeException("updateMyGroupKey: Invalid shared key id or salt, does not match group's value");
        }

        selfMember.setMemberKeyId(publicKeyId);
        selfMember.setEncryptedGroupKey(cryptedSharedKey);
        selfMember.setSharedKeyDate(group.getKeyDate());
        selfMember.setSharedKeyId(sharedKeyId);
        selfMember.setSharedKeyIdSalt(sharedKeyIdSalt);
        changedGroupMember(selfMember, new Date());
    }

    @Override
    public String[] updateGroupKeys(String groupId, String sharedKeyId, String sharedKeyIdSalt, String[] clientIds, String[] publicKeyIds, String[] cryptedSharedKeys) {
        requireIdentification();
        logCall("updateGroupKeys(" + groupId + "/" + sharedKeyId + "," + sharedKeyIdSalt + clientIds.length + " " + publicKeyIds.length + " " + cryptedSharedKeys.length + ")");
        if (!(clientIds.length == publicKeyIds.length && clientIds.length == cryptedSharedKeys.length)) {
            throw new RuntimeException("array length mismatches");
        }
        final TalkGroupMember selfMember = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if (selfMember == null || !(selfMember.isAdmin())) {
            throw new RuntimeException("updateGroupKeys: insufficient permissions");
        }

        List<String> result = new ArrayList<String>();

        if (mDatabase.acquireGroupKeyUpdateLock(groupId, mConnection.getClientId())) { // lock for groupkey update acquired... continuing.
            //try {
                Date now = new Date();
                for (int i = 0; i < clientIds.length; ++i) {
                    TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(groupId, clientIds[i]);
                    if (targetMember == null) {
                        throw new RuntimeException("Client is not a member of group");
                    }
                    targetMember.setMemberKeyId(publicKeyIds[i]);
                    targetMember.setEncryptedGroupKey(cryptedSharedKeys[i]);
                    targetMember.setSharedKeyId(sharedKeyId);
                    targetMember.setSharedKeyIdSalt(sharedKeyIdSalt);
                    targetMember.setKeySupplier(mConnection.getClientId());
                    targetMember.setSharedKeyDate(now);
                    changedGroupMember(targetMember, now);
                }

                TalkGroup group = mDatabase.findGroupById(groupId);
                group.setSharedKeyId(sharedKeyId);
                group.setSharedKeyIdSalt(sharedKeyIdSalt);
                group.setKeySupplier(mConnection.getClientId());
                group.setKeyDate(now);
                changedGroup(group,now);

                String[] activeStates = {
                        TalkGroupMember.STATE_INVITED,
                        TalkGroupMember.STATE_JOINED
                };
                List<TalkGroupMember> members = mDatabase.findGroupMembersByIdWithStates(groupId, activeStates);
                logCall("updateGroupKeys - found " + members.size() + " active group members");

                List<String> outOfDateMembers = new ArrayList<String>();
                for (int i = 0; i < members.size(); ++i) {
                    if (!sharedKeyId.equals(members.get(i).getSharedKeyId())) {
                        outOfDateMembers.add(members.get(i).getClientId());
                    }
                }
                logCall("updateGroupKeys - found " + members.size() + " out of date group members");

                for (int i = 0; i < outOfDateMembers.size(); ++i) {
                    result.add(outOfDateMembers.get(i));
                }

            /*} finally {
                mDatabase.releaseGroupKeyUpdateLock(groupId);
            }*/
        } else { // No lock for groupkey updating was acquired.
            result.add(selfMember.getClientId());
        }

        return result.toArray(new String[result.size()]);
    }
    /*
    @Override
    public void updateGroupMember(TalkGroupMember member) {
        requireIdentification();
        requireGroupAdmin(member.getGroupId());
        logCall("updateGroupMember(groupId: '" + member.getGroupId() + "' / clientId: '" + member.getClientId() + "')");
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(member.getGroupId(), member.getClientId());
        if (targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        targetMember.setRole(member.getRole());
        targetMember.setEncryptedGroupKey(member.getEncryptedGroupKey());
        changedGroupMember(targetMember, new Date());
    }
    */

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

    private void changedGroupMember(TalkGroupMember member, Date changed) {
        member.setLastChanged(changed);
        mDatabase.saveGroupMember(member);
        mServer.getUpdateAgent().requestGroupMembershipUpdate(member.getGroupId(), member.getClientId());
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

    private void requireNotNearbyGroupType(String groupId) {
        // perspectively we should evolve a permission model to enable checking of WHO is allowed to do WHAT in which CONTEXT
        // e.g. client (permission depending on role) inviteGroupMembers to Group (permission depending on type)
        // for now we just check for nearby groups...
        TalkGroup group = mDatabase.findGroupById(groupId);
        if (group.isTypeNearby()) {
            throw new RuntimeException("Group type is: nearby. not allowed.");
        }
    }

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
        LOG.info("updateEnvironment: creating new group for client with id '" + mConnection.getClientId() + "' with type "+environment.getType());
        TalkGroupMember groupAdmin = new TalkGroupMember();
        groupAdmin.setClientId(mConnection.getClientId());
        groupAdmin.setGroupId(group.getGroupId());
        groupAdmin.setRole(TalkGroupMember.ROLE_ADMIN);
        groupAdmin.setState(TalkGroupMember.STATE_JOINED);
        changedGroup(group,new Date());
        changedGroupMember(groupAdmin, group.getLastChanged());

        environment.setGroupId(group.getGroupId());
        environment.setClientId(mConnection.getClientId());
        mDatabase.saveEnvironment(environment);
    }

    private void joinGroupWithEnvironment(TalkGroup group, TalkEnvironment environment) {
        LOG.info("joinGroupWithEnvironment: joining group with client id '" + mConnection.getClientId() + "'");

        TalkGroupMember groupAdmin = mDatabase.findGroupMemberForClient(group.getGroupId(), mConnection.getClientId());
        if (groupAdmin == null) {
            groupAdmin = new TalkGroupMember();
        }
        groupAdmin.setClientId(mConnection.getClientId());
        groupAdmin.setGroupId(group.getGroupId());
        groupAdmin.setRole(TalkGroupMember.ROLE_ADMIN);
        groupAdmin.setState(TalkGroupMember.STATE_JOINED);
        if (!group.getState().equals(TalkGroup.STATE_EXISTS)) {
            group.setState(TalkGroup.STATE_EXISTS);
            mDatabase.saveGroup(group);
        }
        changedGroup(group,new Date());
        changedGroupMember(groupAdmin, group.getLastChanged());

        environment.setGroupId(group.getGroupId());
        environment.setClientId(mConnection.getClientId());
        mDatabase.saveEnvironment(environment);

        touchGroupMemberPresences(group.getGroupId());
        mServer.getUpdateAgent().requestGroupUpdate(group.getGroupId(), mConnection.getClientId());
        mServer.getUpdateAgent().requestGroupMembershipUpdatesForNewMember(group.getGroupId(), mConnection.getClientId());
        mServer.getUpdateAgent().requestPresenceUpdateForGroup(mConnection.getClientId(), group.getGroupId());
        mServer.getUpdateAgent().requestPresenceUpdate(mConnection.getClientId());
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
            environment.setLocationType(TalkEnvironment.TYPE_NEARBY);
        }

        environment.setTimeReceived(new Date());
        environment.setClientId(mConnection.getClientId());

        List<TalkEnvironment> matching = mDatabase.findEnvironmentsMatching(environment);
        TalkEnvironment myEnvironment = mDatabase.findEnvironmentByClientId(environment.getType(),mConnection.getClientId());
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
        if (matching.size() > 0) {
            // join the largest group
            TalkGroup largestGroup = mDatabase.findGroupById(environmentsPerGroup.get(0).getLeft());
            joinGroupWithEnvironment(largestGroup, environment);
            return largestGroup.getGroupId();
        }
        // we are alone or first at the location, lets create a new group
        createGroupWithEnvironment(environment);
        return environment.getGroupId();
    }

    private void destroyEnvironment(TalkEnvironment environment) {
        logCall("destroyEnvironment(" + environment + ")");
        TalkGroup group = mDatabase.findGroupById(environment.getGroupId());
        TalkGroupMember member = mDatabase.findGroupMemberForClient(environment.getGroupId(), environment.getClientId());
        if (member != null && member.getState().equals(TalkGroupMember.STATE_JOINED)) {
            // remove my membership
            // set membership state to NONE
            member.setState(TalkGroupMember.STATE_NONE);
            // degrade removed users to member
            member.setRole(TalkGroupMember.ROLE_MEMBER);
            Date now = new Date();
            changedGroupMember(member, now);
            String[] states = {TalkGroupMember.STATE_JOINED};
            List<TalkGroupMember> membersLeft = mDatabase.findGroupMembersByIdWithStates(environment.getGroupId(), states);
            logCall("destroyEnvironment: membersLeft: " + membersLeft.size());
            if (membersLeft.size() == 0) {
                logCall("destroyEnvironment: last member left, removing group "+group.getGroupId());
                // last member removed, remove group
                group.setState(TalkGroup.STATE_NONE);
                changedGroup(group,now);
                // explicitly request a group updated notification for the last removed client because
                // calling changedGroup only will not send out "groupUpdated" notifications to members with state "none"
                mServer.getUpdateAgent().requestGroupUpdate(group.getGroupId(),environment.getClientId());
            }
        }
        mDatabase.deleteEnvironment(environment);
    }

    @Override
    public void destroyEnvironment(String type) {
        logCall("destroyEnvironment(clientId: '" + mConnection.getClientId() + "')");
        requirePastIdentification();

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
        ArrayList<Boolean> result = new ArrayList<Boolean>();

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
}
