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

    boolean mForceAll;

    public DeliveryRequest(DeliveryAgent agent, String clientId, boolean forceAll) {
        mClientId = clientId;
        mAgent = agent;
        mServer = mAgent.getServer();
        mDatabase = mServer.getDatabase();
        mForceAll = forceAll;
    }


    private boolean performIncoming(List<TalkDelivery> inDeliveries, ITalkRpcClient rpc, TalkRpcConnection connection) {
        boolean currentlyConnected = true;
        for (TalkDelivery delivery : inDeliveries) {
            // we lost the connection somehow
            if (!currentlyConnected) {
                LOG.info("performIncoming: clientId: '" + mClientId + "no longer connected");
                break;
            }

            delivery.ensureDates();
            if (!mForceAll && (delivery.getTimeUpdatedIn().getTime() > delivery.getTimeChanged().getTime())) {
                LOG.info("performIncoming: clientId: '" + mClientId + ",delivery has not changed since last up'd in="+ delivery.getTimeUpdatedIn().getTime()+",changed="+delivery.getTimeChanged().getTime());
                continue;
            }

            synchronized (mServer.idLock(delivery.getMessageId())) {
                // get the matching message
                TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
                if (message == null) {
                    LOG.warn("message not found: " + delivery.getMessageId());
                    continue;
                }
                try {

                    TalkDelivery latestDelivery = mDatabase.findDelivery(delivery.getMessageId(), delivery.getReceiverId());
                    if (latestDelivery == null) {
                        throw new RuntimeException("delivery unexpectedly not found");
                    }

                    // remove for production build
                    if (!latestDelivery.equals(delivery)) {
                        LOG.info("latestDelivery (in) has changed");
                        LOG.info("delivery:"+delivery.toString());
                        LOG.info("latestDelivery:"+latestDelivery.toString());
                    }
                    if (!mForceAll && (latestDelivery.getTimeUpdatedIn().getTime() > latestDelivery.getTimeChanged().getTime())) {
                        LOG.info("performIncoming(2): clientId: '" + mClientId + ", delivery has not changed since last up'd in="+ delivery.getTimeUpdatedIn().getTime()+",changed="+delivery.getTimeChanged().getTime());
                        continue;
                    }

                    // post the delivery for the client

                    boolean recentlyDelivered = (latestDelivery.getTimeUpdatedIn() != null &&
                            latestDelivery.getTimeUpdatedIn().getTime() + 15 * 1000 > new Date().getTime());

                    if (!recentlyDelivered && (TalkDelivery.STATE_DELIVERING.equals(latestDelivery.getState()) /*|| mForceAll*/)) {
                        TalkDelivery filtered = new TalkDelivery();
                        filtered.updateWith(latestDelivery);
                        filtered.setTimeUpdatedIn(null);
                        filtered.setTimeUpdatedOut(null);
                        rpc.incomingDelivery(filtered, message);
                    } else {
                        TalkDelivery filtered = new TalkDelivery();
                        filtered.updateWith(delivery, TalkDelivery.REQUIRED_IN_UPDATE_FIELDS_SET);
                        rpc.incomingDeliveryUpdated(filtered);
                    }
                    latestDelivery.setTimeUpdatedIn(new Date());
                    mDatabase.saveDelivery(latestDelivery);
                } catch (Exception e) {
                    LOG.info("Exception calling incomingDelivery() for clientId: '" + mClientId + "'", e);
                    //currentlyConnected = false; XXX do this when we can differentiate
                }
            }

            // check for disconnects
            if (!connection.isConnected()) {
                currentlyConnected = false;
            }
        }
        return currentlyConnected;
    }

    private boolean performOutgoing(List<TalkDelivery> outDeliveries, ITalkRpcClient rpc, TalkRpcConnection connection) {
        boolean currentlyConnected = true;
        for (TalkDelivery delivery : outDeliveries) {
            // we lost the connection somehow
            if (!currentlyConnected) {
                LOG.info("performOutgoing: clientId: '" + mClientId + "no longer connected");
                break;
            }
            synchronized (mServer.idLock(delivery.getMessageId())) {

                delivery.ensureDates();
                if (!mForceAll && (delivery.getTimeUpdatedOut().getTime() > delivery.getTimeChanged().getTime())) {
                    LOG.info("performOutgoing: clientId: '" + mClientId + ", delivery has not changed since last up'd out="+ delivery.getTimeUpdatedOut().getTime()+",changed="+delivery.getTimeChanged().getTime());
                    continue;
                }

                TalkDelivery latestDelivery = mDatabase.findDelivery(delivery.getMessageId(), delivery.getReceiverId());
                if (latestDelivery == null) {
                    throw new RuntimeException("out delivery unexpectedly not found");
                }

                // remove for production build
                if (!latestDelivery.equals(delivery)) {
                    LOG.info("latestDelivery (out) has changed");
                    LOG.info("delivery:"+delivery.toString());
                    LOG.info("latestDelivery:"+latestDelivery.toString());
                }

                if (!mForceAll && (latestDelivery.getTimeUpdatedOut().getTime() > latestDelivery.getTimeChanged().getTime())) {
                    LOG.info("performOutgoing(2): clientId: '" + mClientId + ", delivery has not changed since last up'd out="+ delivery.getTimeUpdatedOut().getTime()+",changed="+delivery.getTimeChanged().getTime());
                    continue;
                }
                // notify it
                try {
                    TalkDelivery filtered = new TalkDelivery();
                    filtered.updateWith(delivery, TalkDelivery.REQUIRED_OUT_UPDATE_FIELDS_SET);
                    if (filtered.hasValidRecipient() || filtered.isExpandedGroupDelivery()) {
                        rpc.outgoingDeliveryUpdated(filtered);
                        delivery.setTimeUpdatedOut(new Date());
                        mDatabase.saveDelivery(delivery);
                    } else {
                        mDatabase.deleteDelivery(delivery);
                        throw new RuntimeException("delivery is missing group and receiver, deleted");
                    }
                } catch (Exception e) {
                    LOG.info("Exception calling outgoingDelivery() for clientId: '" + mClientId + "'", e);
                }
            }
            // check for disconnects
            if (!connection.isConnected()) {
                currentlyConnected = false;
            }
        }
        return currentlyConnected;
    }


    void perform() {
        LOG.info("DeliverRequest.perform for clientId: '" + mClientId);
        boolean needToNotify = false;
        boolean currentlyConnected = false;

        // determine if the client is currently connected
        TalkRpcConnection connection = mServer.getClientConnection(mClientId);
            ITalkRpcClient rpc = null;
            if (connection != null && connection.isConnected()) {
                currentlyConnected = true;
                rpc = connection.getClientRpc();
            }
        LOG.info("DeliverRequest.perform for clientId: '" + mClientId + ", currentlyConnected=" + currentlyConnected);
        if (currentlyConnected) {

            LOG.debug("DeliverRequest.perform acquiring delivery lock for connection: "+connection.getConnectionId() + "', mClientId="+mClientId);
            synchronized(connection.deliveryLock) {
                LOG.info("DeliverRequest.perform acquired delivery lock for connection: '" + connection.getConnectionId() + "', mClientId=" + mClientId);
                // get all outstanding deliveries for the client
                List<TalkDelivery> inDeliveries =
                        mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
                LOG.info("clientId: '" + mClientId + "' has " + inDeliveries.size() + " incoming deliveries");
                if (!inDeliveries.isEmpty()) {
                    // we will need to push if we don't succeed
                    needToNotify = true;
                    // deliver one by one
                    currentlyConnected = performIncoming(inDeliveries,rpc,connection);
                }

                if (currentlyConnected) {
                    // get all deliveries for the client with not yet completed attachment transfers
                    List<TalkDelivery> inAttachmentDeliveries =
                            mDatabase.findDeliveriesForClientInDeliveryAndAttachmentStates(mClientId, TalkDelivery.IN_ATTACHMENT_DELIVERY_STATES, TalkDelivery.IN_ATTACHMENT_STATES);
                    LOG.info("clientId: '" + mClientId + "' has " + inAttachmentDeliveries.size() + " incoming deliveries with relevant attachment states");
                    if (!inAttachmentDeliveries.isEmpty()) {
                        // we will need to push if we don't succeed
                        // deliver one by one
                        currentlyConnected = performIncoming(inAttachmentDeliveries,rpc,connection);
                    }
                }

                if (currentlyConnected) {
                    List<TalkDelivery> outDeliveries =
                            mDatabase.findDeliveriesFromClientInStates(mClientId, TalkDelivery.OUT_STATES);
                    LOG.info("clientId: '" + mClientId + "' has " + outDeliveries.size() + " outgoing deliveries");
                    if (!outDeliveries.isEmpty())      {
                        // deliver one by one
                        currentlyConnected = performOutgoing(outDeliveries, rpc, connection);
                    }
                }

                if (currentlyConnected) {
                    List<TalkDelivery> outDeliveries =
                            mDatabase.findDeliveriesFromClientInDeliveryAndAttachmentStates(mClientId, TalkDelivery.OUT_ATTACHMENT_DELIVERY_STATES, TalkDelivery.OUT_ATTACHMENT_STATES);
                    LOG.info("clientId: '" + mClientId + "' has " + outDeliveries.size() + " outgoing deliveries with relevant attachment states");
                    if (!outDeliveries.isEmpty())      {
                        // deliver one by one
                        currentlyConnected = performOutgoing(outDeliveries,rpc, connection);
                    }
                }
                mForceAll = false;
            }
        } else {
            List<TalkDelivery> inDeliveries =
                    mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
            LOG.info("unconnected clientId: '" + mClientId + "' has " + inDeliveries.size() + " incoming deliveries");
            if (!inDeliveries.isEmpty()) {
                needToNotify = true;
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
