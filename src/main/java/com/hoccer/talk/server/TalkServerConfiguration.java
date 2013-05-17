package com.hoccer.talk.server;

import com.hoccer.talk.logging.HoccerLoggers;

import java.util.Properties;
import java.util.logging.Logger;

public class TalkServerConfiguration {

    private static final Logger LOG = HoccerLoggers.getLogger(TalkServerConfiguration.class);

    public static final int THREADS_DELIVERY = 1;
    public static final int THREADS_GROUP = 1;
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

    private String mDatabaseBackend = "jongo";

    private String mJongoDb = "talk";

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
        // Database
        mDatabaseBackend = properties.getProperty(PROPERTY_PREFIX + ".db.backend", "jongo");
        // Jongo
        mJongoDb = properties.getProperty(PROPERTY_PREFIX + ".jongo.db", "talk");
    }

    public boolean isGcmEnabled() {
        return mGcmEnabled;
    }

    public String getGcmApiKey() {
        return mGcmApiKey;
    }

    public int getGcmWakeTtl() {
        return mGcmWakeTtl;
    }

    public boolean isApnsEnabled() {
        return mApnsEnabled;
    }

    public boolean isApnsSandbox() {
        return mApnsSandbox;
    }

    public String getApnsCertPath() {
        return mApnsCertPath;
    }

    public String getApnsCertPassword() {
        return mApnsCertPassword;
    }

    public String getDatabaseBackend() {
        return mDatabaseBackend;
    }

    public String getJongoDb() {
        return mJongoDb;
    }

}
