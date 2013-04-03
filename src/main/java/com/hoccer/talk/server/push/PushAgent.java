package com.hoccer.talk.server.push;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.ApnsServiceBuilder;
import com.notnoop.apns.PayloadBuilder;

public class PushAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(PushAgent.class);

	private ExecutorService mExecutor;

	private Sender mGcmSender;

    private ApnsService mApnsService;
	
	public PushAgent() {
		mExecutor = Executors.newSingleThreadExecutor();
        if(TalkServerConfiguration.GCM_ENABLE) {
            initializeGcm();
        }
        if(TalkServerConfiguration.APNS_ENABLE) {
            initializeApns();
        }
    }

    private void initializeGcm() {
        LOG.info("initializing push support for GCM");
        mGcmSender = new Sender(TalkServerConfiguration.GCM_API_KEY);
    }

    private void initializeApns() {
        LOG.info("initializing push support for APNS");
        ApnsServiceBuilder apnsServiceBuilder = APNS.newService()
                .withCert(TalkServerConfiguration.APNS_CERT_PATH,
                          TalkServerConfiguration.APNS_CERT_PASSWORD);
        if(TalkServerConfiguration.APNS_USE_SANDBOX) {
            apnsServiceBuilder = apnsServiceBuilder.withSandboxDestination();
        }
        mApnsService = apnsServiceBuilder.build();
    }
	
	public void submitRequest(final PushRequest request) {
		mExecutor.execute(new Runnable() {
			@Override
			public void run() {
				performRequest(request);
			}
		});
	}

	private void performRequest(PushRequest request) {
		TalkClient client = request.getClient();
        if(TalkServerConfiguration.GCM_ENABLE && client.isGcmCapable()) {
            performRequestViaGcm(request);
        } else if(TalkServerConfiguration.APNS_ENABLE && client.isApnsCapable()) {
            performRequestViaApns(request);
        }
	}

    private void performRequestViaGcm(PushRequest request) {
        LOG.info("push for " + request.getClient().getClientId() + " (GCM)");
        TalkClient client = request.getClient();
        Message message = new Message.Builder()
                .collapseKey("com.hoccer.talk.wake")
                .timeToLive(TalkServerConfiguration.GCM_WAKE_TTL)
                .restrictedPackageName(client.getGcmPackage())
                .dryRun(true)
                .build();
        try {
            mGcmSender.send(message, client.getGcmRegistration(), 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void performRequestViaApns(PushRequest request) {
        LOG.info("push for " + request.getClient().getClientId() + " (APNS)");
        TalkClient client = request.getClient();
        PayloadBuilder b = APNS.newPayload();
        int messageCount = 23;
        if (messageCount > 1) {
            b.localizedKey("apn_new_messages");
            b.localizedArguments(String.valueOf(messageCount));
        } else {
            b.localizedKey("apn_one_new_message");
        }
        b.badge(messageCount);
        b.sound("default");
        mApnsService.push(client.getApnsToken(), b.build());
    }
	
}
