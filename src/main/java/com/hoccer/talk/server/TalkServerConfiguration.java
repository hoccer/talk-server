package com.hoccer.talk.server;

import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

public class TalkServerConfiguration {

    private static final Logger LOG = Logger.getLogger(TalkServerConfiguration.class);

    public static final int THREADS_DELIVERY = 1;
    public static final int THREADS_GROUP = 1;
    public static final int THREADS_UPDATE = 1;
    public static final int THREADS_PUSH = 1;
    public static final int THREADS_PING = 2;

    private static final String PROPERTY_PREFIX = "talk";

    private String mListenAddress = "localhost";
    private int    mListenPort = 8080;

    private boolean mGcmEnabled = false;
    private String  mGcmApiKey = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";
    private int     mGcmWakeTtl = 1 * 7 * 24 * 3600; // 1 week

    private boolean mApnsEnabled = false;
    private boolean mApnsSandbox = false;
    private String  mApnsCertPath = "HoccerTalkApplePushNotificationDev.p12";
    private String  mApnsCertPassword = "password";

    private String mDatabaseBackend = "jongo";

    private String mJongoDb = "talk";

    private String mFilecacheControlUrl = "http://localhost:8081/control";
    private String mFilecacheUploadBase = "http://localhost:8081/upload/";
    private String mFilecacheDownloadBase = "http://localhost:8081/download/";


    public TalkServerConfiguration() {
    }

    public void configureFromProperties(Properties properties) {
        // listening
        mListenAddress = properties.getProperty(PROPERTY_PREFIX + ".listen.address", "localhost");
        mListenPort = Integer.parseInt(properties.getProperty(PROPERTY_PREFIX + ".listen.port", "8080"));
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
        // Filecache
        mFilecacheControlUrl = properties.getProperty(PROPERTY_PREFIX + ".filecache.controlUrl", mFilecacheControlUrl);
        mFilecacheUploadBase = properties.getProperty(PROPERTY_PREFIX + ".filecache.uploadBase", mFilecacheUploadBase);
        mFilecacheDownloadBase = properties.getProperty(PROPERTY_PREFIX + ".filecache.downloadBase", mFilecacheDownloadBase);
    }

    public String getListenAddress() {
        return mListenAddress;
    }

    public int getListenPort() {
        return mListenPort;
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

    public URI getFilecacheControlUrl() {
        URI url = null;
        try {
            url = new URI(mFilecacheControlUrl);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return url;
    }

    public String getFilecacheUploadBase() {
        return mFilecacheUploadBase;
    }

    public String getFilecacheDownloadBase() {
        return mFilecacheDownloadBase;
    }
}
