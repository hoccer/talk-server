package com.hoccer.talk.server.push;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Result;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.PayloadBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * A single in-progress push request
 */
public class PushRequest {

    private static final Logger LOG = Logger.getLogger(PushRequest.class);

    PushAgent mAgent;

    String mClientId;
	TalkClient mClient;

    TalkServerConfiguration mConfig;

	public PushRequest(PushAgent agent, String clientId) {
        mAgent = agent;
        mConfig = mAgent.getConfiguration();
        mClientId = clientId;
	}

    public void perform() {
        LOG.debug("pushing " + mClientId);
        // get up-to-date client object
        mClient = mAgent.getDatabase().findClientById(mClientId);
        if(mClient == null) {
            LOG.warn("client " + mClientId + " does not exist");
            return;
        }
        // try to perform push
        if(mConfig.isGcmEnabled() && mClient.isGcmCapable()) {
            performGcm();
        } else if(mConfig.isApnsEnabled() && mClient.isApnsCapable()) {
            performApns();
        } else {
            if(mClient.isPushCapable()) {
                LOG.warn("client " + mClient + " push not available");
            } else {
                LOG.info("client " + mClientId + " has no registration");
            }
        }
    }

    private void performGcm() {
        LOG.info("GCM push for " + mClientId);
        Message message = new Message.Builder()
                .collapseKey("com.hoccer.talk.wake")
                .timeToLive(mConfig.getGcmWakeTtl())
                .restrictedPackageName(mClient.getGcmPackage())
                .dryRun(false) //TODO: maybe make a setting out of it
                .build();
        Sender gcmSender = mAgent.getGcmSender();
        try {
            Result res = gcmSender.send(message, mClient.getGcmRegistration(), 10);
            if (res.getMessageId() != null) {
                if ( res.getCanonicalRegistrationId() != null ) {
                    LOG.warn("GCM returned a canonical registration id - we should do something with it");
                }
                LOG.debug("GCM push successful, return message id "+res.getMessageId());
            } else {
                LOG.error("GCM push returned error '"+res.getErrorCodeName()+"'");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void performApns() {
        LOG.info("APNS push for " + mClientId);
        ITalkServerDatabase database = mAgent.getDatabase();
        ApnsService apnsService = mAgent.getApnsService(PushAgent.APNS_SERVICE_TYPE.PRODUCTION);
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
