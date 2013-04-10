package com.hoccer.talk.server.delivery;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.push.PushRequest;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.List;
import java.util.logging.Logger;

public class DeliveryRequest {

    private static final Logger LOG = HoccerLoggers.getLogger(DeliveryRequest.class);

    DeliveryAgent mAgent;

    String mClientId;

    TalkServer mServer;

    ITalkServerDatabase mDatabase;

    public DeliveryRequest(DeliveryAgent agent, String clientId) {
        mAgent = agent;
        mClientId = clientId;
        mServer = agent.getServer();
        mDatabase = mServer.getDatabase();
    }

    public void perform() {
        deliverIncomingMessages(mClientId);
    }

    private void deliverIncomingMessages(String clientId) {
        LOG.info("delivering for " + clientId);

        // get all outstanding deliveries for the client - abort if none
        List<TalkDelivery> deliveries = mDatabase.findDeliveriesForClient(clientId);
        if(deliveries.size() == 0) {
            LOG.info("no deliveries pending");
            return;
        } else {
            LOG.info("client has " + deliveries.size() + " deliveries outstanding");
        }

        // deliver directly if connected
        TalkRpcConnection connection = mServer.getClientConnection(clientId);
        if(connection != null && connection.isLoggedIn()) {
            LOG.info("performing direct delivery");
            // perform deliveries one by one
            for(TalkDelivery delivery: deliveries) {
                LOG.info("delivery of " + delivery.getMessageId() + " in state " + delivery.getState());
                // we only care about DELIVERING messages
                if(delivery.getState().equals(TalkDelivery.STATE_DELIVERING)) {
                    // get the matching message
                    TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
                    // post the notification for the client
                    connection.getClientRpc().incomingDelivery(delivery, message);
                }
            }
        }

        // wake client using push services (XXX should only happen when idle for some time)
        LOG.info("performing push");
        // find client in database
        TalkClient client = mDatabase.findClientById(clientId);
        // send push request
        if(client.isGcmCapable() || client.isApnsCapable()) {
            mServer.getPushAgent().submitRequest(new PushRequest(client));
        }

    }

}
