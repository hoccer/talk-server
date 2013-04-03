package com.hoccer.talk.server.rpc;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;


import java.util.List;
import java.util.UUID;
import java.util.Vector;
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

    @Override
    public void registerGcm(String registeredPackage, String registrationId) {
        requireIdentification();
        LOG.info("client registers for GCM with id " + registrationId);
        TalkClient client = mConnection.getClient();
        client.setGcmPackage(registeredPackage);
        client.setGcmRegistration(registrationId);
        mDatabase.saveClient(client);
    }

    @Override
    public void unregisterGcm() {
        requireIdentification();
        LOG.info("client unregisters GCM");
        TalkClient client = mConnection.getClient();
        client.setGcmPackage(null);
        client.setGcmRegistration(null);
        mDatabase.saveClient(client);
    }

    @Override
    public void registerApns(String registrationToken) {
        requireIdentification();
        LOG.info("client registers for APNS with token " + registrationToken);
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
        LOG.info("client unregisters APNS");
        TalkClient client = mConnection.getClient();
        client.setApnsToken(null);
        mDatabase.saveClient(client);
    }

    @Override
    public String[] getAllClients() {
        LOG.info("client gets all clients");
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
        LOG.info("client identifies as " + clientId);
        mConnection.identifyClient(clientId);
        mServer.getDeliveryAgent().triggerDelivery(mConnection.getClientId());
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();

        LOG.info("client requests delivery of new message to " + deliveries.length + " clients");

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
        String clientId = mConnection.getClientId();
        LOG.info("client confirms delivery of message " + messageId);
        TalkDelivery d = mDatabase.findDelivery(messageId, clientId);
        if (d == null) {
            LOG.info("confirmation ignored: no delivery of message "
                    + messageId + " for client " + clientId);
        } else {
            LOG.info("confirmation accepted: message "
                    + messageId + " for client " + clientId);
            d.setState(TalkDelivery.STATE_DELIVERED);
        }
        return d;
    }

}