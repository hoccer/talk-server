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

    private ITalkServerDatabase mDatabase;

    public DeliveryAgent(TalkServer server) {
        mExecutor = Executors.newSingleThreadScheduledExecutor();
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    public void triggerDelivery(final String clientId) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                deliverIncomingMessages(clientId);
            }
        });
    }

    private void deliverIncomingMessages(String clientId) {
        LOG.info("delivering for " + clientId);

        // get all outstanding deliveries for the client - abort if none
        List<TalkDelivery> deliveries = mDatabase.findDeliveriesForClient(clientId);
        if(deliveries.size() == 0) {
            return;
        }

        // deliver directly if connected - push if not connected
        TalkRpcConnection connection = mServer.getClientConnection(clientId);
        if(connection != null && connection.isLoggedIn()) {
            // perform deliveries one by one
            for(TalkDelivery delivery: deliveries) {
                // we only care about DELIVERING messages
                if(delivery.getState() == TalkDelivery.STATE_DELIVERING) {
                    // get the matching message
                    TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
                    // post the notification for the client
                    connection.getClientRpc().incomingDelivery(delivery, message);
                }
            }
        } else {
            // find client in database
            TalkClient client = mDatabase.findClientById(clientId);
            // send push request
            if(client.isGcmCapable() || client.isApnsCapable()) {
                mServer.getPushAgent().submitRequest(new PushRequest(client));
            }
        }

    }

}
