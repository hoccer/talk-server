package com.hoccer.talk.server.delivery;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * Delivery requests encapsulate a delivery run for a given client
 * <p/>
 * Both incoming and outgoing deliveries are handled in one go.
 * If clients are not connected the request is passed on to the push agent.
 * Deliveries are rate-limited to one update every 5 seconds.
 */
public class DeliveryRequest {

    private static final Logger LOG = Logger.getLogger(DeliveryRequest.class);

    String mClientId;

    DeliveryAgent mAgent;

    TalkServer mServer;
    ITalkServerDatabase mDatabase;

    public DeliveryRequest(DeliveryAgent agent, String clientId) {
        mClientId = clientId;
        mAgent = agent;
        mServer = mAgent.getServer();
        mDatabase = mServer.getDatabase();
    }

    void perform() {
        boolean needToNotify = false;
        boolean currentlyConnected = false;

        // determine if the client is currently connected
        TalkRpcConnection connection = mServer.getClientConnection(mClientId);
        ITalkRpcClient rpc = null;
        if (connection != null && connection.isConnected()) {
            currentlyConnected = true;
            rpc = connection.getClientRpc();
        }

        // get all outstanding deliveries for the client
        List<TalkDelivery> inDeliveries =
                mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
        if (!inDeliveries.isEmpty()) {
            LOG.info("has " + inDeliveries.size() + " incoming deliveries");
            // we will need to push if we don't succeed
            needToNotify = true;
            // deliver one by one
            for (TalkDelivery delivery : inDeliveries) {
                // we lost the connection somehow
                if (!currentlyConnected) {
                    break;
                }

                // rate limit
                long now = System.currentTimeMillis();
                long delta = Math.max(0, now - delivery.getTimeUpdatedIn().getTime());
                if (delta < 5000) {
                    continue;
                }

                // get the matching message
                TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
                if (message == null) {
                    LOG.warn("message not found: " + delivery.getMessageId());
                    continue;
                }

                // post the delivery for the client
                try {
                    rpc.incomingDelivery(delivery, message);
                    delivery.setTimeUpdatedIn(new Date());
                    mDatabase.saveDelivery(delivery);
                } catch (Exception e) {
                    LOG.info("Exception calling incomingDelivery()", e);
                    //currentlyConnected = false; XXX do this when we can differentiate
                }

                // check for disconnects
                if (!connection.isConnected()) {
                    currentlyConnected = false;
                }

            }
        }

        List<TalkDelivery> outDeliveries =
                mDatabase.findDeliveriesFromClientInState(mClientId, TalkDelivery.STATE_DELIVERED);
        if (currentlyConnected && !outDeliveries.isEmpty()) {
            LOG.info("has " + outDeliveries.size() + " outgoing deliveries");
            // deliver one by one
            for (TalkDelivery delivery : outDeliveries) {
                // we lost the connection somehow
                if (!currentlyConnected) {
                    break;
                }

                // rate limit
                long now = System.currentTimeMillis();
                long delta = Math.max(0, now - delivery.getTimeUpdatedOut().getTime());
                if (delta < 5000) {
                    continue;
                }

                // notify it
                try {
                    rpc.outgoingDelivery(delivery);
                    delivery.setTimeUpdatedOut(new Date());
                    mDatabase.saveDelivery(delivery);
                } catch (Exception e) {
                    LOG.info("Exception calling outgoingDelivery()");
                }

                // check for disconnects
                if (!connection.isConnected()) {
                    currentlyConnected = false;
                }
            }
        }

        // initiate push delivery if needed
        if (needToNotify && !currentlyConnected) {
            LOG.info("pushing " + mClientId);
            performPush();
        }
    }

    private void performPush() {
        // find client in database
        TalkClient client = mDatabase.findClientById(mClientId);
        // send push request
        if (client.isPushCapable()) {
            mServer.getPushAgent().submitRequest(client);
        } else {
            LOG.info("push unconfigured for " + mClientId);
        }
    }

}
