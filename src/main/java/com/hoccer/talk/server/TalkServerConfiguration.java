package com.hoccer.talk.server;

import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 * Encapsulation of server configuration
 *
 * This gets initialized with defaults and can then
 * be overloaded from a property file.
 */
public class TalkServerConfiguration {

    private static final Logger LOG = Logger.getLogger(TalkServerConfiguration.class);

    public static final boolean LOG_ALL_CALLS = false;

    public static final int THREADS_DELIVERY = 1;
    public static final int THREADS_GROUP = 1;
    public static final int THREADS_UPDATE = 1;
    public static final int THREADS_PUSH = 1;
    public static final int THREADS_PING = 2; // XXX HIGHER COUNT?
    public static final int THREADS_CLEANING = 4;

    private static final String PROPERTY_PREFIX = "talk";

    private String mListenAddress = "localhost";
    private int    mListenPort = 8080;


    private int mPushRateLimit = 15000;
    private boolean mGcmEnabled = false;
    private String  mGcmApiKey = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";
    private int     mGcmWakeTtl = 1 * 7 * 24 * 3600; // 1 week
    private boolean mApnsEnabled = false;
    private boolean mApnsSandbox = false;
    private String  mApnsCertPath = "HoccerTalkApplePushNotificationDev.p12";
    private String  mApnsCertPassword = "password";
    private int     mApnsInvalidateDelay = 30;
    private int     mApnsInvalidateInterval = 3600;

    private String mDatabaseBackend = "jongo";
    private String mJongoDb = "talk";

    private int mCleanupAllClientsDelay = 7200; // 2 hours //300;
    private int mCleanupAllClientsInterval = 60*60*24; // once a day //900;
    private int mCleanupAllDeliveriesDelay = 3600; // 1 hour //600;
    private int mCleanupAllDeliveriesInterval = 60*60*6; // every 6 hours //900;

    private String mFilecacheControlUrl = "http://localhost:8081/control";
    private String mFilecacheUploadBase = "http://localhost:8081/upload/";
    private String mFilecacheDownloadBase = "http://localhost:8081/download/";

    private String mSupportTag = "Oos8guceich2yoox";

    public TalkServerConfiguration() {
    }

    public void configureFromProperties(Properties properties) {
        // listening
        mListenAddress = properties.getProperty(PROPERTY_PREFIX + ".listen.address", mListenAddress);
        mListenPort = Integer.parseInt(properties.getProperty(PROPERTY_PREFIX + ".listen.port", Integer.toString(mListenPort)));

        // Database
        mDatabaseBackend = properties.getProperty(PROPERTY_PREFIX + ".db.backend", mDatabaseBackend);
        // Jongo
        mJongoDb = properties.getProperty(PROPERTY_PREFIX + ".jongo.db", mJongoDb);

        // Push
        mPushRateLimit = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".push.rateLimit", Integer.toString(mPushRateLimit)));
        // APNS
        mApnsEnabled = Boolean.valueOf(properties.getProperty(PROPERTY_PREFIX + ".apns.enabled", Boolean.toString(mApnsEnabled)));
        mApnsSandbox = Boolean.valueOf(properties.getProperty(PROPERTY_PREFIX + ".apns.sandbox", Boolean.toString(mApnsSandbox)));
        mApnsCertPath = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.path", mApnsCertPath);
        mApnsCertPassword = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.password", mApnsCertPassword);
        mApnsInvalidateDelay = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".apns.invalidate.delay", Integer.toString(mApnsInvalidateDelay)));
        mApnsInvalidateInterval = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".apns.invalidate.interval", Integer.toString(mApnsInvalidateInterval)));
        // GCM
        mGcmEnabled = Boolean.valueOf(properties.getProperty(PROPERTY_PREFIX + ".gcm.enabled", Boolean.toString(mGcmEnabled)));
        mGcmApiKey  = properties.getProperty(PROPERTY_PREFIX + ".gcm.apikey", mGcmApiKey);

        // Cleanup
        mCleanupAllClientsDelay = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".cleanup.allClientsDelay", Integer.toString(mCleanupAllClientsDelay)));
        mCleanupAllClientsInterval = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".cleanup.allClientsInterval", Integer.toString(mCleanupAllClientsInterval)));
        mCleanupAllDeliveriesDelay = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".cleanup.allDeliveriesDelay", Integer.toString(mCleanupAllDeliveriesDelay)));
        mCleanupAllDeliveriesInterval = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".cleanup.allDeliveriesInterval", Integer.toString(mCleanupAllDeliveriesInterval)));

        // Filecache
        mFilecacheControlUrl = properties.getProperty(PROPERTY_PREFIX + ".filecache.controlUrl", mFilecacheControlUrl);
        mFilecacheUploadBase = properties.getProperty(PROPERTY_PREFIX + ".filecache.uploadBase", mFilecacheUploadBase);
        mFilecacheDownloadBase = properties.getProperty(PROPERTY_PREFIX + ".filecache.downloadBase", mFilecacheDownloadBase);

        // Support
        mSupportTag = properties.getProperty(PROPERTY_PREFIX + ".support.tag", mSupportTag);
    }

    public String getListenAddress() {
        return mListenAddress;
    }

    public int getListenPort() {
        return mListenPort;
    }

    public String getDatabaseBackend() {
        return mDatabaseBackend;
    }

    public String getJongoDb() {
        return mJongoDb;
    }

    public int getPushRateLimit() {
        return mPushRateLimit;
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

    public int getApnsInvalidateDelay() {
        return mApnsInvalidateDelay;
    }

    public int getApnsInvalidateInterval() {
        return mApnsInvalidateInterval;
    }

    public URI getFilecacheControlUrl() {
        URI url = null;
        try {
            url = new URI(mFilecacheControlUrl);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return url;
    }

    public int getCleanupAllClientsDelay() {
        return mCleanupAllClientsDelay;
    }

    public int getCleanupAllClientsInterval() {
        return mCleanupAllClientsInterval;
    }

    public int getCleanupAllDeliveriesDelay() {
        return mCleanupAllDeliveriesDelay;
    }

    public int getCleanupAllDeliveriesInterval() {
        return mCleanupAllDeliveriesInterval;
    }

    public String getFilecacheUploadBase() {
        return mFilecacheUploadBase;
    }

    public String getFilecacheDownloadBase() {
        return mFilecacheDownloadBase;
    }

    public String getSupportTag() {
        return mSupportTag;
    }

}
