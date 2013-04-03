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
        mGcmSender = new Sender(TalkServerConfiguration.GCM_API_KEY);
    }

    private void initializeApns() {
        ApnsServiceBuilder apnsServiceBuilder = APNS.newService()
                .withCert(TalkServerConfiguration.APNS_CERT_PATH,
                          TalkServerConfiguration.APNS_CERT_PASSWORD);
        if(TalkServerConfiguration.APNS_USE_SANDBOX) {
            apnsServiceBuilder = apnsServiceBuilder.withSandboxDestination();
        }
        mApnsService = apnsServiceBuilder.build();
    }
	
	public void submitRequest(final PushRequest request) {
        LOG.info("submitted request for " + request.getClient().getClientId());
		mExecutor.execute(new Runnable() {
			@Override
			public void run() {
				performRequest(request);
			}
		});
	}

	private void performRequest(PushRequest request) {
        LOG.info("performing push for " + request.getClient().getClientId());
		TalkClient client = request.getClient();
        if(TalkServerConfiguration.GCM_ENABLE && client.isGcmCapable()) {
            performRequestViaGcm(request);
        } else if(TalkServerConfiguration.APNS_ENABLE && client.isApnsCapable()) {
            performRequestViaApns(request);
        }
	}

    private void performRequestViaGcm(PushRequest request) {
        LOG.info("performing GCM push for " + request.getClient().getClientId());
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
        LOG.info("performing APNS push for " + request.getClient().getClientId());
        TalkClient client = request.getClient();
        PayloadBuilder b = APNS.newPayload();
        b.alertBody("You have new messages!");
        b.sound("default");
        mApnsService.push(client.getApnsToken(), b.build());
    }
	
}
