package com.hoccer.talk.server.delivery;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DeliveryRequest {

    private static final Logger LOG = HoccerLoggers.getLogger(DeliveryRequest.class);

    String mClientId;

    DeliveryAgent mAgent;

    TalkServer mServer;
    ITalkServerDatabase mDatabase;

    public DeliveryRequest(DeliveryAgent agent, String clientId) {
        mClientId = clientId;
        mAgent = agent;
        mServer = agent.getServer();
        mDatabase = mServer.getDatabase();
    }

    void perform() {
        LOG.info("notifying " + mClientId);
        boolean needToNotify = false;
        boolean currentlyConnected = false;

        // determine if the client is currently connected
        TalkRpcConnection connection = mServer.getClientConnection(mClientId);
        ITalkRpcClient rpc = null;
        if(connection != null && connection.isConnected()) {
            currentlyConnected = true;
            rpc = connection.getClientRpc();
        }

        // get all outstanding deliveries for the client - abort if none
        List<TalkDelivery> deliveries =
                mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
        if(deliveries.size() > 0) {
            // we need to push if we don't succeed
            needToNotify = true;
            // deliver if we can
            if(currentlyConnected) {
                for(TalkDelivery delivery: deliveries) {
                    // get the matching message
                    TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
                    // post the delivery for the client
                    try {
                        rpc.incomingDelivery(delivery, message);
                    } catch (Exception e) {
                        LOG.log(Level.INFO, "Exception while notifying", e);
                        //currentlyConnected = false; XXX do this when we can differentiate
                    }
                    // check for disconnects
                    if(!connection.isConnected()) {
                        currentlyConnected = false;
                    }
                    // we lost the connection somehow
                    if(!currentlyConnected) {
                        break;
                    }
                }
            }
        }

        // XXX get all outstanding out-deliveries and update

        // initiate push delivery if needed
        if(needToNotify && !currentlyConnected) {
            LOG.info("pushing " + mClientId);
            performPush();
        }
    }

    private void performPush() {
        // find client in database
        TalkClient client = mDatabase.findClientById(mClientId);
        // send push request
        if(client.isGcmCapable() || client.isApnsCapable()) {
            mServer.getPushAgent().submitRequest(client);
        } else {
            LOG.info("push unconfigured for " + mClientId);
        }
    }

}
