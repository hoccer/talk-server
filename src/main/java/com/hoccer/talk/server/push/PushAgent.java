package com.hoccer.talk.server.push;

import java.util.Date;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.ApnsServiceBuilder;
import org.apache.log4j.Logger;

public class PushAgent {

    private static final Logger LOG = Logger.getLogger(PushAgent.class);

	private ScheduledExecutorService mExecutor;

    TalkServer mServer;
    TalkServerConfiguration mConfig;
    ITalkServerDatabase mDatabase;

	private Sender mGcmSender;

    private ApnsService mApnsService;

    Hashtable<String, PushRequest> mOutstanding;

    AtomicInteger mPushRequests = new AtomicInteger();
    AtomicInteger mPushDelayed = new AtomicInteger();
    AtomicInteger mPushIncapable = new AtomicInteger();

    public PushAgent(TalkServer server) {
		mExecutor = Executors.newScheduledThreadPool(TalkServerConfiguration.THREADS_PUSH);
        mServer = server;
        mDatabase = mServer.getDatabase();
        mConfig = mServer.getConfiguration();
        mOutstanding = new Hashtable<String, PushRequest>();
        if(mConfig.isGcmEnabled()) {
            initializeGcm();
        }
        if(mConfig.isApnsEnabled()) {
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
    }

    public void submitRequest(TalkClient client) {
        long now = System.currentTimeMillis();

        mPushRequests.incrementAndGet();

        // bail if no push
        if(!client.isPushCapable()) {
            mPushIncapable.incrementAndGet();
            return;
        }

        // determine delay
        Date lastPush = client.getTimeLastPush();
        if(lastPush == null) {
            lastPush = new Date();
        }
        long delta = Math.max(0, now - lastPush.getTime());
        long delay = 0;
        if(delta < 5000) {
            mPushDelayed.incrementAndGet();
            delay = 5000 - delta;
        }
        client.setTimeLastPush(new Date());
        mDatabase.saveClient(client);

        // schedule the request
        final String clientId = client.getClientId();
        synchronized (mOutstanding) {
            if(!mOutstanding.contains(clientId)) {
                final PushRequest request = new PushRequest(this, clientId);
                mExecutor.schedule(new Runnable() {
                    @Override
                    public void run() {
                        mOutstanding.remove(clientId);
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

    public ApnsService getApnsService() {
        return mApnsService;
    }

    private void initializeGcm() {
        LOG.info("GCM support enabled");
        mGcmSender = new Sender(mConfig.getGcmApiKey());
    }

    private void initializeApns() {
        LOG.info("APNS support enabled");
        ApnsServiceBuilder apnsServiceBuilder = APNS.newService()
                .withCert(mConfig.getApnsCertPath(),
                          mConfig.getApnsCertPassword());
        if(mConfig.isApnsSandbox()) {
            apnsServiceBuilder = apnsServiceBuilder.withSandboxDestination();
        } else {
            apnsServiceBuilder = apnsServiceBuilder.withProductionDestination();
        }
        mApnsService = apnsServiceBuilder.build();
        invalidateApns();
    }

    private void invalidateApns() {
        LOG.info("APNS retrieving inactive devices");
        Map<String, Date> inactive = mApnsService.getInactiveDevices();
        LOG.info("APNS reports " + inactive.size() + " inactive devices");
        for(String token: inactive.keySet()) {
            TalkClient client = mDatabase.findClientByApnsToken(token);
            if(client == null) {
                LOG.info("unknown inactive APNS client with token " + token);
            } else {
                LOG.info("client inactive on APNS since " + inactive.get(token) + ": " + client.getClientId());
                client.setApnsToken(null);
            }
        }
    }

}
