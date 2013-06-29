package com.hoccer.talk.server.delivery;

import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DeliveryAgent {

    private static final Logger LOG = Logger.getLogger(DeliveryAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    public DeliveryAgent(TalkServer server) {
        mExecutor = Executors.newScheduledThreadPool(TalkServerConfiguration.THREADS_DELIVERY);
        mServer = server;
    }

    public TalkServer getServer() {
        return mServer;
    }

    public void requestDelivery(String clientId) {
        final DeliveryRequest request = new DeliveryRequest(this, clientId);
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    request.perform();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        });
    }

}
