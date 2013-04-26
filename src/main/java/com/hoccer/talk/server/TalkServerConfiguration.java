package com.hoccer.talk.server;

import com.hoccer.talk.logging.HoccerLoggers;

import java.util.Properties;
import java.util.logging.Logger;

public class TalkServerConfiguration {

    private static final Logger LOG = HoccerLoggers.getLogger(TalkServerConfiguration.class);

    public static final int THREADS_DELIVERY = 1;
    public static final int THREADS_UPDATE = 1;
    public static final int THREADS_PUSH = 1;
    public static final int THREADS_PING = 2;

    private static final String PROPERTY_PREFIX = "talk";


    private boolean mGcmEnabled = false;
    private String  mGcmApiKey = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";
    private int     mGcmWakeTtl = 1 * 7 * 24 * 3600; // 1 week

    private boolean mApnsEnabled = false;
    private boolean mApnsSandbox = false;
    private String  mApnsCertPath = "HoccerTalkApplePushNotificationDev.p12";
    private String  mApnsCertPassword = "password";

    public TalkServerConfiguration() {
    }

    public void configureFromProperties(Properties properties) {
        // APNS
        mApnsEnabled = properties.getProperty(PROPERTY_PREFIX + ".apns.enabled", "false").equals("true");
        mApnsSandbox = properties.getProperty(PROPERTY_PREFIX + ".apns.sandbox", "true").equals("true");
        mApnsCertPath = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.path", "apns.p12");
        mApnsCertPassword = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.password", "password");
        // GCM
        mGcmEnabled = properties.getProperty(PROPERTY_PREFIX + ".gcm.enabled", "false").equals("true");
        mGcmApiKey  = properties.getProperty(PROPERTY_PREFIX + ".gcm.apikey", "ABCD");
    }

    public boolean ismGcmEnabled() {
        return mGcmEnabled;
    }

    public String getmGcmApiKey() {
        return mGcmApiKey;
    }

    public int getmGcmWakeTtl() {
        return mGcmWakeTtl;
    }

    public boolean ismApnsEnabled() {
        return mApnsEnabled;
    }

    public boolean ismApnsSandbox() {
        return mApnsSandbox;
    }

    public String getmApnsCertPath() {
        return mApnsCertPath;
    }

    public String getmApnsCertPassword() {
        return mApnsCertPassword;
    }

}
