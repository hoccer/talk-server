package com.hoccer.talk.server.agents;

import com.hoccer.talk.util.NamedThreadFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class NotificationDeferrer {

    private Executor mExecutor;

    public NotificationDeferrer(int poolSize, String poolName) {

        mExecutor = Executors.newScheduledThreadPool(
                poolSize,
                new NamedThreadFactory(poolName)
        );
    }

    protected final Logger LOG = Logger.getLogger(getClass());

    protected void queueOrExecute(ThreadLocal<ArrayList<Runnable>> context, Runnable notificationGenerator) {
        // TODO: Maybe a Queue instead of an ArrayList would be more appropriate here.

        if (context.get() != null) {
            ArrayList<Runnable> queue = context.get();
            LOG.debug("context is currently set (" + queue.size() + " items). Queueing notification generator.");
            queue.add(notificationGenerator);
        } else {
            LOG.debug("context is currently NOT set. Immediately executing notification generators");
            mExecutor.execute(notificationGenerator);
        }
    }

    private void flushContext(ThreadLocal<ArrayList<Runnable>> context) {
        LOG.debug("Flushing context.");
        if (context.get() != null) {
            ArrayList<Runnable> queue = context.get();

            if (queue.size() > 0) {
                LOG.debug("  * " + queue.size() + " notification generators were queued. flushing them...");
                for (Runnable notification : queue) {
                    mExecutor.execute(notification);
                }
            } else {
                LOG.debug("  * No notification generators were queued - nothing to do.");
            }
        }
        context.remove();
    }

    protected void setRequestContext(ThreadLocal<ArrayList<Runnable>> context) {
        LOG.debug("Setting context.");
        if (context.get() != null) {
            LOG.warn("context still contains notification generators! Flushing(executing) them now.");
            flushContext(context);
        }
        context.set(new ArrayList<Runnable>());
    }

    protected void clearRequestContext(ThreadLocal<ArrayList<Runnable>> context) {
        LOG.debug("Clearing context.");
        flushContext(context);
    }

}
