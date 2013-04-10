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

    public DeliveryAgent(TalkServer server) {
        mExecutor = Executors.newSingleThreadScheduledExecutor();
        mServer = server;
    }

    public TalkServer getServer() {
        return mServer;
    }

    public void triggerDelivery(final String clientId) {
        final DeliveryRequest request = new DeliveryRequest(this, clientId);
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                request.perform();
            }
        });
    }

}
