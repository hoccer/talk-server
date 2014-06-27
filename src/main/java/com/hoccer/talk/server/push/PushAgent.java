package com.hoccer.talk.server.push;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.util.NamedThreadFactory;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Agent for push notifications
 */
public class PushAgent {

    private static final Logger LOG = Logger.getLogger(PushAgent.class);

    private final ScheduledExecutorService mExecutor;

    TalkServer mServer;
    TalkServerConfiguration mConfig;
    ITalkServerDatabase mDatabase;

    private Sender mGcmSender;

    public enum APNS_SERVICE_TYPE {
        PRODUCTION, SANDBOX
    }
    private final HashMap<APNS_SERVICE_TYPE, ApnsService> mApnsServices = new HashMap<APNS_SERVICE_TYPE, ApnsService>();

    Hashtable<String, PushRequest> mOutstanding;

    AtomicInteger mPushRequests = new AtomicInteger();
    AtomicInteger mPushDelayed = new AtomicInteger();
    AtomicInteger mPushIncapable = new AtomicInteger();
    AtomicInteger mPushBatched = new AtomicInteger();

    public PushAgent(TalkServer server) {
        mExecutor = Executors.newScheduledThreadPool(
            TalkServerConfiguration.THREADS_PUSH,
            new NamedThreadFactory("push-agent")
        );
        mServer = server;
        mDatabase = mServer.getDatabase();
        mConfig = mServer.getConfiguration();
        mOutstanding = new Hashtable<String, PushRequest>();
        if (mConfig.isGcmEnabled()) {
            initializeGcm();
        }
        if (mConfig.isApnsEnabled()) {
            initializeApns();
        }
        initializeMetrics(mServer.getMetrics());
    }

    private void initializeMetrics(MetricRegistry metrics) {
        metrics.register(MetricRegistry.name(PushAgent.class, "pushRequests"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPushRequests.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PushAgent.class, "pushIncapable"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPushIncapable.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PushAgent.class, "pushDelayed"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPushDelayed.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PushAgent.class, "pushBatched"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPushBatched.intValue();
                    }
                });
    }

    public void submitSystemMessage(final TalkClient client, final String message) {
        LOG.info("submitSystemMessage -> clientId: '" + client.getClientId() + "' message: '" + message + "'");
        final PushMessage pushMessage = new PushMessage(this, client, message);
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                LOG.info(" -> initializing Message Push for clientId: '" + client.getClientId() +"'!");
                pushMessage.perform();
                LOG.info(" -> Message Push done for clientId: '" + client.getClientId() +"'!");
            }
        }, 0, TimeUnit.MILLISECONDS);
    }

    public void submitRequest(TalkClient client) {
        long now = System.currentTimeMillis();

        mPushRequests.incrementAndGet();

        // bail if no push
        if (!client.isPushCapable()) {
            mPushIncapable.incrementAndGet();
            return;
        }

        // limit push rate
        Date lastPush = client.getTimeLastPush();
        if (lastPush == null) {
            lastPush = new Date();
        }
        long delta = Math.max(0, now - lastPush.getTime());
        long delay = 0;
        int limit = mConfig.getPushRateLimit();
        if (delta < limit) {
            mPushDelayed.incrementAndGet();
            delay = Math.max(0, limit - delta);
        }

        // update timestamp
        client.setTimeLastPush(new Date());
        mDatabase.saveClient(client);

        // only perform push when we aren't doing so already
        final String clientId = client.getClientId();
        synchronized (mOutstanding) {
            if (mOutstanding.containsKey(clientId)) {
                // request has been batched
                mPushBatched.incrementAndGet();
            } else {
                // schedule the request
                final PushRequest request = new PushRequest(this, clientId);
                mExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        // no longer outstanding
                        mOutstanding.remove(clientId);
                        // perform the request
                        request.perform();
                    }
                }, delay, TimeUnit.MILLISECONDS);
                mOutstanding.put(clientId, request);
            }
        }
    }

    public TalkServerConfiguration getConfiguration() {
        return mConfig;
    }

    public ITalkServerDatabase getDatabase() {
        return mDatabase;
    }

    public Sender getGcmSender() {
        return mGcmSender;
    }

    public ApnsService getApnsService(APNS_SERVICE_TYPE type) {
        return mApnsServices.get(type);
    }

    private void initializeGcm() {
        LOG.info("GCM support enabled");
        mGcmSender = new Sender(mConfig.getGcmApiKey());
    }

    private void initializeApns() {
        LOG.info("APNS support enabled");

        // set up services
        LOG.info("  * setting up APNS service (type: '" + APNS_SERVICE_TYPE.PRODUCTION + "')");
        mApnsServices.put(APNS_SERVICE_TYPE.PRODUCTION, APNS.newService()
                .withCert(mConfig.getApnsCertProductionPath(),
                        mConfig.getApnsCertProductionPassword())
                .withProductionDestination()
                .build());
        LOG.info("  * setting up APNS service (type: '" + APNS_SERVICE_TYPE.SANDBOX + "')");
        mApnsServices.put(APNS_SERVICE_TYPE.SANDBOX,    APNS.newService()
                .withCert(mConfig.getApnsCertSandboxPath(),
                        mConfig.getApnsCertSandboxPassword())
                .withSandboxDestination()
                .build());

        // set up invalidation
        int delay = mConfig.getApnsInvalidateDelay();
        int interval = mConfig.getApnsInvalidateInterval();
        if (interval > 0) {
            LOG.info("APNS will check for invalidations every " + interval + " seconds");
            mExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    invalidateApns();
                }
            }, delay, interval, TimeUnit.SECONDS);
        }
    }

    private void invalidateApns() {
        LOG.info("APNS retrieving inactive devices");
        Iterator iterator = mApnsServices.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pairs = (Map.Entry) iterator.next();
            LOG.info("  * APNS retrieving inactive devices from " + pairs.getKey());
            final ApnsService service = (ApnsService)pairs.getValue();
            final Map<String, Date> inactive = service.getInactiveDevices();
            if (!inactive.isEmpty()) {
                LOG.info("  * APNS reports " + inactive.size() + " inactive devices");
                for (String token : inactive.keySet()) {
                    TalkClient client = mDatabase.findClientByApnsToken(token);
                    if (client == null) {
                        LOG.warn("    * APNS invalidates unknown client (token '" + token + "')");
                    } else {
                        LOG.info("    * APNS client '" + client.getClientId() + "' invalid since '" + inactive.get(token) + "'");
                        client.setApnsToken(null);
                    }
                }
            }
        }
    }
}
