package com.hoccer.talk.server.delivery;

import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.agents.NotificationDeferrer;

import java.util.ArrayList;

public class DeliveryAgent extends NotificationDeferrer {

    private static final ThreadLocal<ArrayList<Runnable>> context = new ThreadLocal<ArrayList<Runnable>>();

    private final TalkServer mServer;

    public DeliveryAgent(TalkServer server) {
        super(
            TalkServerConfiguration.THREADS_DELIVERY,
            "delivery-agent"
        );
        mServer = server;
    }

    public TalkServer getServer() {
        return mServer;
    }

    public void requestDelivery(String clientId, boolean forceAll) {
        final DeliveryRequest deliveryRequest = new DeliveryRequest(this, clientId, forceAll);

        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                try {
                    deliveryRequest.perform();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };

        queueOrExecute(context, notificationGenerator);
    }

    public void setRequestContext() {
        setRequestContext(context);
    }

    public void clearRequestContext() {
        clearRequestContext(context);
    }
}
