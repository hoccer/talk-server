package com.hoccer.talk.server.push;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.PayloadBuilder;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class PushRequest {

    private static final Logger LOG = HoccerLoggers.getLogger(PushAgent.class);

    PushAgent mAgent;

	TalkClient mClient;

    TalkServerConfiguration mConfig;

	public PushRequest(PushAgent agent, TalkClient client) {
        mAgent = agent;
		mClient = client;
        mConfig = mAgent.getConfiguration();
	}
	
	public TalkClient getClient() {
		return mClient;
	}

    public void perform() {
        if(mConfig.ismGcmEnabled() && mClient.isGcmCapable()) {
            performGcm();
        } else if(mConfig.ismApnsEnabled() && mClient.isApnsCapable()) {
            performApns();
        }
    }

    private void performGcm() {
        LOG.info("push GCM " + mClient.getClientId());
        Message message = new Message.Builder()
                .collapseKey("com.hoccer.talk.wake")
                .timeToLive(mConfig.getmGcmWakeTtl())
                .restrictedPackageName(mClient.getGcmPackage())
                .dryRun(true)
                .build();
        Sender gcmSender = mAgent.getGcmSender();
        try {
            gcmSender.send(message, mClient.getGcmRegistration(), 10);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void performApns() {
        LOG.info("push APNS " + mClient.getClientId());
        ITalkServerDatabase database = mAgent.getDatabase();
        ApnsService apnsService = mAgent.getApnsService();
        PayloadBuilder b = APNS.newPayload();
        List<TalkDelivery> deliveries =
                database.findDeliveriesForClientInState(
                        mClient.getClientId(),
                        TalkDelivery.STATE_DELIVERING);
        int messageCount = 0;
        messageCount += mClient.getApnsUnreadMessages();
        messageCount += (deliveries == null) ? 0 : deliveries.size();
        if (messageCount > 1) {
            b.localizedKey("apn_new_messages");
            b.localizedArguments(String.valueOf(messageCount));
        } else {
            b.localizedKey("apn_one_new_message");
        }
        b.badge(messageCount);
        b.sound("default");
        apnsService.push(mClient.getApnsToken(), b.build());
    }
	
}
