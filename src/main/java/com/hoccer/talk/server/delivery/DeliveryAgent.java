package com.hoccer.talk.server.delivery;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.server.TalkServer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

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

    public void requestDelivery(String clientId) {
        LOG.info("requesting " + clientId);
        final DeliveryRequest request = new DeliveryRequest(this, clientId);
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                request.perform();
            }
        });
    }

}
