package com.hoccer.talk.server.delivery;

import com.hoccer.talk.logging.HoccerLoggers;
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

public class DeliveryRequest {

    private static final Logger LOG = Logger.getLogger(DeliveryRequest.class);

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

        // get all outstanding deliveries for the client
        List<TalkDelivery> inDeliveries =
                mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
        if(inDeliveries.size() > 0) {
            // we need to push if we don't succeed
            needToNotify = true;
            // deliver if we can
            if(currentlyConnected) {
                for(TalkDelivery delivery: inDeliveries) {
                    // rate limit
                    long now = System.currentTimeMillis();
                    long delta = Math.max(0, now - delivery.getTimeUpdatedIn().getTime());
                    if(delta < 5000) {
                        continue;
                    }
                    // get the matching message
                    TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
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

        List<TalkDelivery> outDeliveries =
                mDatabase.findDeliveriesFromClientInState(mClientId, TalkDelivery.STATE_DELIVERED);
        if(outDeliveries.size() > 0) {
            if(currentlyConnected) {
                for(TalkDelivery delivery: outDeliveries) {
                    // rate limit
                    long now = System.currentTimeMillis();
                    long delta = Math.max(0, now - delivery.getTimeUpdatedOut().getTime());
                    if(delta < 5000) {
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
        if(client.isPushCapable()) {
            mServer.getPushAgent().submitRequest(client);
        } else {
            LOG.info("push unconfigured for " + mClientId);
        }
    }

}
