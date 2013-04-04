package com.hoccer.talk.server.rpc;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.model.TalkToken;
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
    public void identify(String clientId) {
        logCall("identify(" + clientId + ")");
        mConnection.identifyClient(clientId);
        mServer.getDeliveryAgent().triggerDelivery(mConnection.getClientId());
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
        ProcessBuilder pb = new ProcessBuilder("pwgen", "20", "1");
        try {
            Process p = pb.start();
            InputStream s = p.getInputStream();
            BufferedReader r = new BufferedReader(new InputStreamReader(s));
            String line = r.readLine();
            LOG.info("pwline " + line);
            if(line.length() == 20) {
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

        LOG.info("performing token-based pairing between " + mConnection.getClientId() + " and " + token.getClientId());

        // invalidate the token
        token.setState(TalkToken.STATE_USED);
        mDatabase.saveToken(token);

        return true;
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();

        logCall("deliveryRequest(" + deliveries.length + " deliveries)");

        // generate a message id
        String messageId = UUID.randomUUID().toString();
        message.setSenderId(mConnection.getClientId());
        message.setMessageId(messageId);

        // walk deliveries and determine which to accept,
        // filling in missing things as we go
        Vector<TalkDelivery> acceptedDeliveries = new Vector<TalkDelivery>();
        for (TalkDelivery d : deliveries) {
            String receiverId = d.getReceiverId();
            // initialize the mid field
            d.setMessageId(messageId);

            // reject messages to self
            if (receiverId.equals(mConnection.getClientId())) {
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