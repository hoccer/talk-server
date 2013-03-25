package com.hoccer.talk.server.delivery;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.server.TalkDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * This class is responsible for ongoing deliveries
 */
public class DeliveryAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(DeliveryAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    public DeliveryAgent(TalkServer server) {
        mExecutor = Executors.newSingleThreadScheduledExecutor();
        mServer = server;
    }

    public void triggerDelivery(final String clientId) {
        LOG.info("triggerDelivery(" + clientId + ")");
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                deliverIncomingMessages(clientId);
            }
        });
    }

    private void deliverIncomingMessages(String clientId) {
        LOG.info("deliverIncomingMessages(" + clientId + ")");
        // we can only do something if there is an active connection
        TalkRpcConnection connection = mServer.getClientConnection(clientId);
        if(connection != null && connection.isLoggedIn()) {
            // get all outstanding deliveries for the client
            List<TalkDelivery> deliveries = TalkDatabase.findDeliveriesForClient(clientId);
            for(TalkDelivery delivery: deliveries) {
                // we only care about DELIVERING messages
                if(delivery.getState() == TalkDelivery.STATE_DELIVERING) {
                    // get the matching message
                    TalkMessage message = TalkDatabase.findMessage(delivery.getMessageId());
                    // post the notification for the client
                    connection.getClientRpc().incomingDelivery(delivery, message);
                }
            }
        } else {
            LOG.info("can not deliver to client - not connected");
        }
    }

}
