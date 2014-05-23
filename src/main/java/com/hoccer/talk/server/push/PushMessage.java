package com.hoccer.talk.server.push;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.PayloadBuilder;
import org.apache.log4j.Logger;

import java.util.List;

public class PushMessage {

    private static final Logger LOG = Logger.getLogger(PushMessage.class);
    private final String mMessage;
    private final TalkClient mClient;

    PushAgent mAgent;
    TalkServerConfiguration mConfig;

    public PushMessage(PushAgent agent, TalkClient client, String message) {
        this.mClient = client;
        this.mMessage = message;

        this.mAgent = agent;
        this.mConfig = mAgent.getConfiguration();
    }

    // Currently implemented only for APNS
    public void perform() {
        LOG.info("performing push to clientId: '" + mClient.getClientId() + "', message: '" + mMessage + "'");
        if(mConfig.isGcmEnabled() && mClient.isGcmCapable()) {
            performGcm();
        } else if(mConfig.isApnsEnabled() && mClient.isApnsCapable()) {
            performApns();
        } else {
            if(mClient.isPushCapable()) {
                LOG.warn("client " + mClient + " push not available");
            } else {
                LOG.info("client " + mClient.getClientId() + " has no registration");
            }
        }

    }

    private void performApns() {
        LOG.info("performApns: to clientId: '" + mClient.getClientId() + "', message: '" + mMessage + "'");
        ApnsService apnsService = mAgent.getApnsService();
        PayloadBuilder b = APNS.newPayload();

        b.alertBody(mMessage);
        b.sound("default");
        apnsService.push(mClient.getApnsToken(), b.build());
    }

    private void performGcm() {
        LOG.warn("performGcm: Currently unsupported! Doing nothing.");
    }

}
