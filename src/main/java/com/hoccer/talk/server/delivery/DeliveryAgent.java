package com.hoccer.talk.server.delivery;

import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.util.NamedThreadFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class DeliveryAgent {

    private final Logger LOG = Logger.getLogger(getClass());

    private static final ThreadLocal<ArrayList<Runnable>> context = new ThreadLocal<ArrayList<Runnable>>();

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    public DeliveryAgent(TalkServer server) {
        mExecutor = Executors.newScheduledThreadPool(
            TalkServerConfiguration.THREADS_DELIVERY,
            new NamedThreadFactory("delivery-agent")
        );
        mServer = server;
    }

    public TalkServer getServer() {
        return mServer;
    }

    public void requestDelivery(String clientId) {
        final DeliveryRequest deliveryRequest = new DeliveryRequest(this, clientId);

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

        queueOrExecute(notificationGenerator);
    }

    private void queueOrExecute(Runnable notificationGenerator) {
        if (context.get() != null) {
            ArrayList<Runnable> queue = context.get();
            LOG.info("context is currently set (" + queue.size() + " items). Queueing notification generator.");
            queue.add(notificationGenerator);
        } else {
            LOG.info("context is currently NOT set. Immediately executing notification generators");
            mExecutor.execute(notificationGenerator);
        }
    }

    private void flushContext() {
        LOG.debug("Flushing context.");
        if (context.get() != null) {
            ArrayList<Runnable> queue = context.get();

            if (queue.size() > 0) {
                LOG.info("  * " + queue.size() + " notification generators were queued. flushing them...");
                for (Runnable notification : queue) {
                    mExecutor.execute(notification);
                }
            } else {
                LOG.info("  * No notification generators were queued - nothing to do.");
            }
        }
        context.remove();
    }

    public void setRequestContext() {
        LOG.info("Setting context.");
        if (context.get() != null) {
            LOG.warn("context still contains notification generators! Flushing(executing) them now.");
            flushContext();
        }
        context.set(new ArrayList<Runnable>());
    }

    public void clearRequestContext() {
        LOG.info("Clearing context.");
        flushContext();
    }

}
