package com.hoccer.talk.server.push;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.ApnsServiceBuilder;
import com.notnoop.apns.PayloadBuilder;

public class PushAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(PushAgent.class);

	private ScheduledExecutorService mExecutor;

    TalkServer mServer;
    ITalkServerDatabase mDatabase;

	private Sender mGcmSender;

    private ApnsService mApnsService;
	
	public PushAgent(TalkServer server) {
		mExecutor = Executors.newScheduledThreadPool(2);
        mServer = server;
        mDatabase = mServer.getDatabase();
        if(TalkServerConfiguration.GCM_ENABLE) {
            initializeGcm();
        }
        if(TalkServerConfiguration.APNS_ENABLE) {
            initializeApns();
        }
    }

    public void submitRequest(TalkClient client) {
        final PushRequest request = new PushRequest(this, client);
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                request.perform();
            }
        }, 5, TimeUnit.SECONDS);
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
        mGcmSender = new Sender(TalkServerConfiguration.GCM_API_KEY);
    }

    private void initializeApns() {
        LOG.info("APNS support enabled");
        ApnsServiceBuilder apnsServiceBuilder = APNS.newService()
                .withCert(TalkServerConfiguration.APNS_CERT_PATH,
                          TalkServerConfiguration.APNS_CERT_PASSWORD);
        if(TalkServerConfiguration.APNS_USE_SANDBOX) {
            apnsServiceBuilder = apnsServiceBuilder.withSandboxDestination();
        }
        mApnsService = apnsServiceBuilder.build();
    }

}
