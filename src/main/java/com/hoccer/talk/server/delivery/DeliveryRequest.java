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
                break;
            }

            delivery.ensureDates();
            if (!mForceAll && (delivery.getTimeUpdatedIn().getTime() > delivery.getTimeChanged().getTime())) {
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
                    LOG.info("performOutgoing: clientId: '" + mClientId + "delivery has not changed since last out");
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
                    LOG.info("performOutgoing: clientId: '" + mClientId + "delivery has not changed since last out (2)");
                    continue;
                }
                // notify it
                try {
                    TalkDelivery filtered = new TalkDelivery();
                    filtered.updateWith(delivery, TalkDelivery.REQUIRED_OUT_UPDATE_FIELDS_SET);
                    rpc.outgoingDeliveryUpdated(filtered);
                    delivery.setTimeUpdatedOut(new Date());
                    mDatabase.saveDelivery(delivery);
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

    // attachment states the receiver is interested in
    final static String[] IN_ATTACHMENT_DELIVERY_STATES = {TalkDelivery.STATE_DELIVERED, TalkDelivery.STATE_DELIVERED_ACKNOWLEDGED};
    final static String[] IN_ATTACHMENT_STATES = {TalkDelivery.ATTACHMENT_STATE_UPLOADING, TalkDelivery.ATTACHMENT_STATE_UPLOADED,
            TalkDelivery.ATTACHMENT_STATE_UPLOAD_PAUSED, TalkDelivery.ATTACHMENT_STATE_UPLOAD_ABORTED, TalkDelivery.ATTACHMENT_STATE_UPLOAD_FAILED};

    // attachment states the sender is interested in
    final static String[] OUT_ATTACHMENT_DELIVERY_STATES = {TalkDelivery.STATE_DELIVERED_ACKNOWLEDGED};
    final static String[] OUT_ATTACHMENT_STATES = {TalkDelivery.ATTACHMENT_STATE_RECEIVED, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_ABORTED, TalkDelivery.ATTACHMENT_STATE_DOWNLOAD_FAILED};

    // The delivery states the sender is interested in
    public static final String[] OUT_STATES = {TalkDelivery.STATE_DELIVERED, TalkDelivery.STATE_FAILED, TalkDelivery.STATE_REJECTED};

    // The delivery states the receiver is interested in
    public static final String[] IN_STATES = {TalkDelivery.STATE_DELIVERING};

    //public static final String[] ALL_STATES = {STATE_NEW, STATE_DELIVERING, STATE_DELIVERED,
    //        STATE_DELIVERED_ACKNOWLEDGED, STATE_FAILED, STATE_ABORTED, STATE_REJECTED, STATE_FAILED_ACKNOWLEDGED, STATE_ABORTED_ACKNOWLEDGED,
    //        STATE_REJECTED_ACKNOWLEDGED};

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
        if (currentlyConnected) {

            synchronized(connection.deliveryLock) {
                // get all outstanding deliveries for the client
                List<TalkDelivery> inDeliveries =
                        mDatabase.findDeliveriesForClientInState(mClientId, TalkDelivery.STATE_DELIVERING);
                if (!inDeliveries.isEmpty()) {
                    LOG.info("clientId: '" + mClientId + "' has " + inDeliveries.size() + " incoming deliveries");
                    // we will need to push if we don't succeed
                    needToNotify = true;
                    // deliver one by one
                    currentlyConnected = performIncoming(inDeliveries,rpc,connection);
                }

                if (currentlyConnected) {
                    // get all deliveries for the client with not yet completed attachment transfers
                    List<TalkDelivery> inAttachmentDeliveries =
                            mDatabase.findDeliveriesForClientInDeliveryAndAttachmentStates(mClientId, IN_ATTACHMENT_DELIVERY_STATES, IN_ATTACHMENT_STATES);
                    if (!inAttachmentDeliveries.isEmpty()) {
                        LOG.info("clientId: '" + mClientId + "' has " + inAttachmentDeliveries.size() + " incoming deliveries with relevant attachment stetes");
                        // we will need to push if we don't succeed
                        // deliver one by one
                        currentlyConnected = performIncoming(inAttachmentDeliveries,rpc,connection);
                    }
                }

                if (currentlyConnected) {
                    List<TalkDelivery> outDeliveries =
                            mDatabase.findDeliveriesFromClientInStates(mClientId, OUT_STATES);
                    if (!outDeliveries.isEmpty())      {
                        LOG.info("clientId: '" + mClientId + "' has " + outDeliveries.size() + " outgoing deliveries");
                        // deliver one by one
                        currentlyConnected = performOutgoing(outDeliveries, rpc, connection);
                    }
                }

                if (currentlyConnected) {
                    List<TalkDelivery> outDeliveries =
                            mDatabase.findDeliveriesFromClientInDeliveryAndAttachmentStates(mClientId, OUT_ATTACHMENT_DELIVERY_STATES, OUT_ATTACHMENT_STATES);
                    if (!outDeliveries.isEmpty())      {
                        LOG.info("clientId: '" + mClientId + "' has " + outDeliveries.size() + " outgoing deliveries with relevant attachment stetes");
                        // deliver one by one
                        currentlyConnected = performOutgoing(outDeliveries,rpc, connection);
                    }
                }

                // initiate push delivery if needed
                if (needToNotify && !currentlyConnected) {
                    LOG.info("pushing " + mClientId);
                    performPush();
                }
                mForceAll = false;
            }
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
