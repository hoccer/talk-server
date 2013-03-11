package com.hoccer.talk.server;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.TalkRpcServer;

import java.util.List;
import java.util.logging.Logger;

public class TalkRpcHandler implements TalkRpcServer {

    private static final Logger LOG =
            HoccerLoggers.getLogger(TalkRpcHandler.class);

    /** Reference to server */
    private TalkServer mServer;

    /** Reference to connection object */
    private TalkRpcConnection mConnection;

    /** Client object, if logged in */
    private TalkClient mClient;

    public TalkRpcHandler(TalkServer pServer, TalkRpcConnection pConnection) {
        mServer = pServer;
        mConnection = pConnection;
    }

    private void requireIdentification() {
        if (mClient == null) {
            throw new RuntimeException("Not logged in");
        }
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
        mClient = TalkDatabase.findClient(clientId);
        mServer.identifyClient(mClient, mConnection);
    }

    @Override
    public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
        requireIdentification();
        LOG.info("client requests delivery of new message to "
                + deliveries.length + " clients");
        TalkDatabase.saveMessage(message);
        for (TalkDelivery d : deliveries) {
            String receiverId = d.getReceiverId();
            if (receiverId.equals(mClient.getClientId())) {
                LOG.info("delivery rejected: send to self");
                // mark delivery failed
            } else {
                TalkClient receiver = TalkDatabase.findClient(receiverId);
                if (receiver == null) {
                    LOG.info("delivery rejected: client " + receiverId + " does not exist");
                    // mark delivery failed
                } else {
                    LOG.info("delivery accepted: client " + receiverId);
                    // delivery accepted, save
                    TalkDatabase.saveDelivery(d);
                }
            }
        }
        return deliveries;
    }

    @Override
    public TalkDelivery deliveryConfirm(String messageId) {
        requireIdentification();
        LOG.info("client confirms delivery of message " + messageId);
        TalkDelivery d = TalkDatabase.findDelivery(messageId, mClient.getClientId());
        if (d == null) {
            LOG.info("confirmation ignored: no delivery of message "
                    + messageId + " for client " + mClient.getClientId());
        } else {
            LOG.info("confirmation accepted: message "
                    + messageId + " for client " + mClient.getClientId());
        }
        return d;
    }

}