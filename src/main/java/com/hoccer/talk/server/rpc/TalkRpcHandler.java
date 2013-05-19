package com.hoccer.talk.server.rpc;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.*;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.srp.SRP6Parameters;
import com.hoccer.talk.srp.SRP6VerifyingServer;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.CryptoException;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.SHA256Digest;

import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;
import java.util.logging.Logger;

/**
 * RPC handler for talk protocol communications
 *
 * This class has all of its public methods exposed directly to the client
 * via JSON-RPC. It should not hold any state, only process calls.
 *
 */
public class TalkRpcHandler implements ITalkRpcServer {

    private static final Logger LOG =
            HoccerLoggers.getLogger(TalkRpcHandler.class);

    private static Hex HEX = new Hex();
    private final Digest SRP_DIGEST = new SHA256Digest();
    private static final SecureRandom SRP_RANDOM = new SecureRandom();
    private static final SRP6Parameters SRP_PARAMETERS = SRP6Parameters.CONSTANTS_1024;

    /** Reference to server */
    private TalkServer mServer;

    /** Reference to database accessor */
    private ITalkServerDatabase mDatabase;

    /** Reference to connection object */
    private TalkRpcConnection mConnection;

    /** SRP authentication state */
    private SRP6VerifyingServer mSrpServer;

    /** SRP authenticating user */
    private TalkClient mSrpClient;

    public TalkRpcHandler(TalkServer pServer, TalkRpcConnection pConnection) {
        mServer = pServer;
        mConnection = pConnection;
        mDatabase = mServer.getDatabase();
    }

    private void requireIdentification() {
        if (!mConnection.isLoggedIn()) {
            throw new RuntimeException("Not logged in");
        }
    }

    private void logCall(String message) {
        LOG.info("[" + mConnection.getConnectionId() + "] " + message);
    }

    @Override
    public String generateId() {
        logCall("generateId()");

        if(mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't register while logged in");
        }
        if(mConnection.isRegistering()) {
            throw new RuntimeException("Can't register more than one identity per connection");
        }

        String clientId = UUID.randomUUID().toString();

        mConnection.beginRegistration(clientId);

        return clientId;
    }

    @Override
    public String srpRegister(String verifier, String salt) {
        logCall("srpRegister(" + verifier + "," + salt + ")");

        if(mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't register while logged in");
        }

        String clientId = mConnection.getUnregisteredClientId();

        if(clientId == null) {
            throw new RuntimeException("You need to generate an id before registering");
        }

        // XXX check verifier and salt for viability

        TalkClient client = new TalkClient();
        client.setClientId(clientId);
        client.setSrpSalt(salt);
        client.setSrpVerifier(verifier);
        client.setTimeRegistered(new Date());

        mDatabase.saveClient(client);

        return clientId;
    }

    @Override
    public String srpPhase1(String clientId, String A) {
        logCall("srpPhase1(" + clientId + "," + A + ")");

        // check if we aren't logged in already
        if(mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't authenticate while logged in");
        }

        // create SRP state
        if(mSrpServer == null) {
            mSrpServer = new SRP6VerifyingServer();
        } else {
            throw new RuntimeException("Can only attempt SRP once per connection");
        }

        // get client object
        mSrpClient = mDatabase.findClientById(clientId);
        if(mSrpClient == null) {
            throw new RuntimeException("No such client");
        }

        // verify SRP registration
        if(mSrpClient.getSrpVerifier() == null || mSrpClient.getSrpSalt() == null) {
            throw new RuntimeException("No such client");
        }

        // parse the salt from DB
        byte[] salt = null;
        try {
            salt = (byte[]) HEX.decode(mSrpClient.getSrpSalt());
        } catch (DecoderException e) {
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        }

        // return our credentials for the client
        return credentials.toString(16);
    }

    @Override
    public String srpPhase2(String M1) {
        logCall("srpPhase2(" + M1 + ")");

        // check if we aren't logged in already
        if(mConnection.isLoggedIn()) {
            throw new RuntimeException("Can't authenticate while logged in");
        }

        // verify we are in a good state to do phase2
        if(mSrpServer == null) {
            throw new RuntimeException("Need to perform phase 1 first");
        }
        if(mSrpClient == null) {
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
        byte[] M2 = mSrpServer.verifyClient(M1b);
        if(M2 == null) {
            throw new RuntimeException("Verification failed");
        }

        // we are now logged in
        mConnection.identifyClient(mSrpClient.getClientId());

        // clear SRP state
        mSrpClient = null;
        mSrpServer = null;

        // return server evidence for client to check
        return Hex.encodeHexString(M2);
    }

    @Override
    public TalkServerInfo hello(TalkClientInfo clientInfo) {
        requireIdentification();
        logCall("hello()");
        TalkServerInfo si = new TalkServerInfo();
        si.setServerTime(new Date());
        return si;
    }

    @Override
    public void registerGcm(String registeredPackage, String registrationId) {
        requireIdentification();
        logCall("registerGcm(" + registeredPackage + "," + registrationId + ")");
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
        logCall("registerApns(" + registrationToken + ")");
        // APNS occasionally returns these for no good reason
        if(registrationToken.length() == 0) {
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
        logCall("hintApnsUnreadMessages(" + numUnreadMessages + ")");
        TalkClient client = mConnection.getClient();
        client.setApnsUnreadMessages(numUnreadMessages);
        mDatabase.saveClient(client);
    }

    @Override
    public TalkRelationship[] getRelationships(Date lastKnown) {
        requireIdentification();

        logCall("getRelationships(" + lastKnown.toString() + ")");

        // query the db
        List<TalkRelationship> relationships =
                mDatabase.findRelationshipsChangedAfter(mConnection.getClientId(), lastKnown);

        // build the result array
        TalkRelationship[] res = new TalkRelationship[relationships.size()];
        int idx = 0;
        for(TalkRelationship r: relationships) {
            res[idx++] = r;
        }

        // return the bunch
        return res;
    }

    @Override
    public void identify(String clientId) {
        logCall("identify(" + clientId + ")");

        // client is now considered to be logged in
        mConnection.identifyClient(clientId);
    }

    @Override
    public void updatePresence(TalkPresence presence) {
        requireIdentification();

        logCall("updatePresence()");

        // find existing presence or create one
        TalkPresence existing = mDatabase.findPresenceForClient(mConnection.getClientId());
        if(existing == null) {
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

        logCall("getPresences(" + lastKnown + ")");

        // perform the query
        List<TalkPresence> pres = mDatabase.findPresencesChangedAfter(mConnection.getClientId(), lastKnown);

        // update connection status and convert results to array
        TalkPresence[] res = new TalkPresence[pres.size()];
        for(int i = 0; i < res.length; i++) {
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
        if(k != null) {
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

        logCall("getKey(" + clientId + "," + keyId + ")");

        TalkKey res = null;

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if(rel != null && rel.getState().equals(TalkRelationship.STATE_FRIEND)) {
            res = mDatabase.findKey(clientId, keyId);
        } else {
            throw new RuntimeException("Given client is not your friend");
        }

        return res;
    }

    @Override
    public String generateToken(String tokenPurpose, int secondsValid) {
        requireIdentification();

        logCall("generateToken(" + tokenPurpose + "," + secondsValid + ")");

        // verify request
        if(!TalkToken.isValidPurpose(tokenPurpose)) {
            throw new RuntimeException("Invalid token purpose");
        }

        // constrain validity period (XXX constants)
        secondsValid = Math.max(60, secondsValid);            // at least 1 minute
        secondsValid = Math.min(7 * 24 * 3600, secondsValid); // at most 1 week

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
            if(secret != null) {
                LOG.warning("Token generator returned existing token - regenerating");
            }
            if(attempt++ > 3) {
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

        // save the token
        mDatabase.saveToken(token);

        // return the secret
        return token.getSecret();
    }

    private String genPw() {
        String result = null;
        ProcessBuilder pb = new ProcessBuilder("pwgen", "10", "1");
        try {
            Process p = pb.start();
            InputStream s = p.getInputStream();
            BufferedReader r = new BufferedReader(new InputStreamReader(s));
            String line = r.readLine();
            LOG.info("pwline " + line);
            if(line.length() == 10) {
                result = line;
            }
        } catch (IOException ioe) {
        }
        return result;
    }

    @Override
    public boolean pairByToken(String secret) {
        requireIdentification();
        logCall("pairByToken(" + secret + ")");

        TalkToken token = mDatabase.findTokenByPurposeAndSecret(
                            TalkToken.PURPOSE_PAIRING, secret);

        // check if token exists
        if(token == null) {
            LOG.info("no token could be found");
            return false;
        }

        // check if token is unused
        if(!token.getState().equals(TalkToken.STATE_UNUSED)) {
            LOG.info("token has already been used");
            return false;
        }

        // check if token is still valid
        if(token.getExpiryTime().before(new Date())) {
            LOG.info("token has expired");
            return false;
        }

        // get relevant client IDs
        String myId = mConnection.getClientId();
        String otherId = token.getClientId();

        // log about it
        LOG.info("performing token-based pairing between " + mConnection.getClientId() + " and " + token.getClientId());

        // set up relationships
        setRelationship(myId, otherId, TalkRelationship.STATE_FRIEND, true);
        setRelationship(otherId, myId, TalkRelationship.STATE_FRIEND, true);

        // invalidate the token
        token.setState(TalkToken.STATE_USED);
        mDatabase.saveToken(token);

        return true;
    }

    @Override
    public void blockClient(String clientId) {
        requireIdentification();

        logCall("blockClient(" + clientId + ")");

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if(rel == null) {
            throw new RuntimeException("You are not paired with client " + clientId);
        }

        String oldState = rel.getState();

        if(oldState.equals(TalkRelationship.STATE_FRIEND)) {
            setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_BLOCKED, true);
            return;
        }
        if(oldState.equals(TalkRelationship.STATE_BLOCKED)) {
            return;
        }

        throw new RuntimeException("You are not paired with client " + clientId);
    }

    @Override
    public void unblockClient(String clientId) {
        requireIdentification();

        logCall("unblockClient(" + clientId + ")");

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if(rel == null) {
            throw new RuntimeException("You are not paired with client " + clientId);
        }

        String oldState = rel.getState();

        if(oldState.equals(TalkRelationship.STATE_FRIEND)) {
            return;
        }
        if(oldState.equals(TalkRelationship.STATE_BLOCKED)) {
            setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_FRIEND, true);
            return;
        }

        throw new RuntimeException("You are not paired with client " + clientId);
    }

    @Override
    public void depairClient(String clientId) {
        requireIdentification();

        logCall("depairClient(" + clientId + ")");

        TalkRelationship rel = mDatabase.findRelationshipBetween(mConnection.getClientId(), clientId);
        if(rel == null) {
            return;
        }

        setRelationship(mConnection.getClientId(), clientId, TalkRelationship.STATE_NONE, false);
        setRelationship(clientId, mConnection.getClientId(), TalkRelationship.STATE_NONE, true);
    }

    private void setRelationship(String thisClientId, String otherClientId, String state, boolean notify) {
        if(!TalkRelationship.isValidState(state)) {
            throw new RuntimeException("Invalid state " + state);
        }
        LOG.info("relationship between " + thisClientId + " and " + otherClientId + " is now " + state);
        TalkRelationship relationship = mDatabase.findRelationshipBetween(thisClientId, otherClientId);
        if(relationship == null) {
            relationship = new TalkRelationship();
        }
        relationship.setClientId(thisClientId);
        relationship.setOtherClientId(otherClientId);
        relationship.setState(state);
        relationship.setLastChanged(new Date());
        mDatabase.saveRelationship(relationship);
        if(notify) {
            mServer.getUpdateAgent().requestRelationshipUpdate(relationship);
        }
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();

        logCall("deliveryRequest(" + deliveries.length + " deliveries)");

        // who is doing this again?
        String clientId = mConnection.getClientId();

        // get the current date for stamping
        Date currentDate = new Date();

        // generate a message id
        String messageId = UUID.randomUUID().toString();
        message.setSenderId(clientId);
        message.setMessageId(messageId);

        // walk deliveries and determine which to accept,
        // filling in missing things as we go
        Vector<TalkDelivery> acceptedDeliveries = new Vector<TalkDelivery>();
        for (TalkDelivery d : deliveries) {
            String receiverId = d.getReceiverId();
            // initialize the mid field
            d.setMessageId(messageId);
            // set the sender for easier db retrieval
            d.setSenderId(clientId);

            // reject messages to self
            if (receiverId.equals(clientId)) {
                LOG.info("delivery rejected: send to self");
                // mark delivery failed
                d.setState(TalkDelivery.STATE_FAILED);
                continue;
            }

            // reject messages to nonexisting clients
            //   XXX this check does not currently work because findClient() creates instances
            TalkClient receiver = mDatabase.findClientById(receiverId);
            if (receiver == null) {
                LOG.info("delivery rejected: client " + receiverId + " does not exist");
                // mark delivery failed
                d.setState(TalkDelivery.STATE_FAILED);
                continue;
            }

            // find relationship between clients, if there is one
            TalkRelationship relationship = mDatabase.findRelationshipBetween(receiverId, clientId);

            // reject if there is no relationship
            if (relationship == null) {
                LOG.info("delivery rejected: client " + receiverId + " has no relationship with sender");
                d.setState(TalkDelivery.STATE_FAILED);
                continue;
            }

            // reject unless befriended
            if (!relationship.getState().equals(TalkRelationship.STATE_FRIEND)) {
                LOG.info("delivery rejected: client " + receiverId
                        + " is not a friend of sender (relationship is " + relationship.getState() + ")");
                d.setState(TalkDelivery.STATE_FAILED);
                continue;
            }

            // all fine, delivery accepted
            LOG.info("delivery accepted: client " + receiverId);
            // mark delivery as in progress
            d.setState(TalkDelivery.STATE_DELIVERING);
            // set delivery timestamps
            d.setTimeAccepted(currentDate);
            d.setTimeChanged(currentDate);
            // delivery accepted, remember as such
            acceptedDeliveries.add(d);
        }

        // process all accepted deliveries
        if(!acceptedDeliveries.isEmpty()) {
            // save the message
            mDatabase.saveMessage(message);
            for(TalkDelivery ds: acceptedDeliveries) {
                // save the delivery object
                mDatabase.saveDelivery(ds);
                // initiate delivery
                mServer.getDeliveryAgent().requestDelivery(ds.getReceiverId());
            }
        }

        // done - return whatever we are left with
        return deliveries;
    }

    @Override
    public TalkDelivery deliveryConfirm(String messageId) {
        requireIdentification();
        logCall("deliveryConfirm(" + messageId + ")");
        String clientId = mConnection.getClientId();
        TalkDelivery d = mDatabase.findDelivery(messageId, clientId);
        if (d != null) {
            if(d.getState().equals(TalkDelivery.STATE_DELIVERING)) {
                LOG.info("confirmed " + messageId + " for " + clientId);
                setDeliveryState(d, TalkDelivery.STATE_DELIVERED);
            } else {
                LOG.info("reconfirmed " + messageId + " for " + clientId);
            }
        }
        return d;
    }

    @Override
    public TalkDelivery deliveryAcknowledge(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAcknowledge(" + messageId + "," + recipientId + ")");
        TalkDelivery d = mDatabase.findDelivery(messageId, recipientId);
        if (d != null) {
            if(d.getState().equals(TalkDelivery.STATE_DELIVERED)) {
                LOG.info("acknowledged " + messageId + " for " + recipientId);
                setDeliveryState(d, TalkDelivery.STATE_CONFIRMED);
            } else {
                LOG.info("reacknowledged " + messageId + " for " + recipientId);
            }
        }
        return d;
    }

    @Override
    public TalkDelivery deliveryAbort(String messageId, String recipientId) {
        requireIdentification();
        logCall("deliveryAbort(" + messageId + "," + recipientId);
        String clientId = mConnection.getClientId();
        TalkDelivery delivery = mDatabase.findDelivery(messageId, recipientId);
        if(delivery != null) {
            if(recipientId.equals(clientId)) {
                // abort incoming delivery, regardless of sender
                setDeliveryState(delivery, TalkDelivery.STATE_ABORTED);
            } else {
                // abort outgoing delivery iff we are the actual sender
                if(delivery.getSenderId().equals(clientId)) {
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
        if(state.equals(TalkDelivery.STATE_DELIVERED)) {
            mServer.getDeliveryAgent().requestDelivery(delivery.getSenderId());
        }
        if(state.equals(TalkDelivery.STATE_DELIVERING)) {
            mServer.getDeliveryAgent().requestDelivery(delivery.getReceiverId());
        }
    }

    @Override
    public String createGroup(String groupTag) {
        requireIdentification();
        logCall("createGroup(" + groupTag + ")");
        TalkGroupMember gm = new TalkGroupMember();
        gm.setClientId(mConnection.getClientId());
        gm.setGroupId(UUID.randomUUID().toString());
        gm.setRole(TalkGroupMember.ROLE_ADMIN);
        changedGroupMember(gm);
        return gm.getGroupId();
    }

    @Override
    public void deleteGroup(String groupId) {
        requireIdentification();
        logCall("deleteGroup(" + groupId + ")");
        requiredGroupAdmin(groupId);

        // walk the group and make everyone have a "none" relationship to it
        List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
        for(TalkGroupMember member: members) {
            member.setRole(TalkGroupMember.ROLE_NONE);
            changedGroupMember(member);
        }
    }

    @Override
    public void addGroupMember(TalkGroupMember member) {
        requireIdentification();
        logCall("addGroupMember(" + member.getGroupId() + "/" + member.getClientId() + ")");
        requiredGroupAdmin(member.getGroupId());
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(member.getGroupId(), member.getClientId());
        if(targetMember != null) {
            throw new RuntimeException("Already a member");
        }
        mDatabase.saveGroupMember(targetMember);
        changedGroupMember(member);
    }

    @Override
    public void removeGroupMember(TalkGroupMember member) {
        requireIdentification();
        logCall("removeGroupMember(" + member.getGroupId() + "/" + member.getClientId() + ")");
        requiredGroupAdmin(member.getGroupId());
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(member.getGroupId(), member.getClientId());
        if(targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        targetMember.setRole(TalkGroupMember.ROLE_NONE);
        changedGroupMember(targetMember);
    }

    @Override
    public void updateGroupMember(TalkGroupMember member) {
        requireIdentification();
        logCall("updateGroupMember(" + member.getGroupId() + "/" + member.getClientId() + ")");
        requiredGroupAdmin(member.getGroupId());
        TalkGroupMember targetMember = mDatabase.findGroupMemberForClient(member.getGroupId(), member.getClientId());
        if(targetMember == null) {
            throw new RuntimeException("Client is not a member of group");
        }
        targetMember.setRole(member.getRole());
        targetMember.setGroupKeyCipherText(member.getGroupKeyCipherText());
        targetMember.setInvitationSecret(member.getInvitationSecret());
        changedGroupMember(targetMember);
    }

    @Override
    public TalkGroupMember[] getGroupMembers(String groupId, Date lastKnown) {
        requireIdentification();
        logCall("getGroupMembers(" + groupId + "/" + lastKnown + ")");
        requiredGroupMember(groupId);

        List<TalkGroupMember> members = mDatabase.findGroupMembersByIdChangedAfter(groupId, lastKnown);
        TalkGroupMember[] res = new TalkGroupMember[members.size()];
        for(int i = 0; i < res.length; i++) {
            res[i] = members.get(i);
        }
        return res;
    }

    private void changedGroupMember(TalkGroupMember member) {
        mDatabase.saveGroupMember(member);
        mServer.getGroupAgent().requestGroupUpdate(member.getGroupId(), member.getClientId());
    }

    private TalkGroupMember requiredGroupAdmin(String groupId) {
        TalkGroupMember gm = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if(gm != null && (gm.getRole().equals(TalkGroupMember.ROLE_ADMIN))) {
            return gm;
        }
        throw new RuntimeException("Client is not an admin in group " + groupId);
    }

    private TalkGroupMember requiredGroupMember(String groupId) {
        TalkGroupMember gm = mDatabase.findGroupMemberForClient(groupId, mConnection.getClientId());
        if(gm != null &&
                (gm.getRole().equals(TalkGroupMember.ROLE_ADMIN)
                   || gm.getRole().equals(TalkGroupMember.ROLE_MEMBER))) {
            return gm;
        }
        throw new RuntimeException("Client is not a member in group " + groupId);
    }

}