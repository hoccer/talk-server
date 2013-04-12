package com.hoccer.talk.server.rpc;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.*;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;


import java.io.*;
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

    /** Reference to server */
    private TalkServer mServer;

    /** Reference to database accessor */
    private ITalkServerDatabase mDatabase;

    /** Reference to connection object */
    private TalkRpcConnection mConnection;

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
    public String srpPhase1(String clientId, String A) {
        throw new RuntimeException("Nope, authentication isn't implemented yet");
    }

    @Override
    public String srpPhase2(String M1) {
        throw new RuntimeException("Nope, authentication isn't implemented yet");
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
    public String[] getAllClients() {
        logCall("getAllClients()");
        List<String> ri = mServer.getAllClients();
        String[] r = new String[ri.size()];
        int i = 0;
        for (String s : ri) {
            r[i++] = s;
        }
        return r;
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

        // tell the client if it doesn't have push
        if(!mConnection.getClient().isPushCapable()) {
            mConnection.getClientRpc().pushNotRegistered();
        }

        // attempt to deliver anything we might have
        mServer.getDeliveryAgent().triggerDelivery(mConnection.getClientId());
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
        mServer.getPresenceAgent().requestPresenceUpdate(mConnection.getClientId(), TalkPresence.CONN_STATUS_ONLINE);
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
        setRelationship(myId, otherId, TalkRelationship.STATE_FRIEND);
        setRelationship(otherId, myId, TalkRelationship.STATE_FRIEND);

        // invalidate the token
        token.setState(TalkToken.STATE_USED);
        mDatabase.saveToken(token);

        return true;
    }

    private void setRelationship(String thisClientId, String otherClientId, String state) {
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
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();

        logCall("deliveryRequest(" + deliveries.length + " deliveries)");

        // who is doing this again?
        String clientId = mConnection.getClientId();

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
                mServer.getDeliveryAgent().triggerDelivery(ds.getReceiverId());
            }
        }

        // done - return whatever we are left with
        return deliveries;
    }

    @Override
    public TalkDelivery deliveryConfirm(String messageId) {
        requireIdentification();
        logCall("confirmDelivery(" + messageId + ")");
        String clientId = mConnection.getClientId();
        TalkDelivery d = mDatabase.findDelivery(messageId, clientId);
        if (d == null) {
            LOG.info("confirmation ignored: no delivery of message "
                    + messageId + " for client " + clientId);
        } else {
            LOG.info("confirmation accepted: message "
                    + messageId + " for client " + clientId);
            d.setState(TalkDelivery.STATE_DELIVERED);
            mDatabase.saveDelivery(d);
        }
        return d;
    }

}