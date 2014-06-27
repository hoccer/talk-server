package com.hoccer.talk.server;

import com.hoccer.scm.GitInfo;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Encapsulation of server configuration
 *
 * This gets initialized with defaults and can then
 * be overloaded from a property file.
 */

// TODO: maybe use Lombok's @Data and @ToString ?
public class TalkServerConfiguration {

    private static final Logger LOG = Logger.getLogger(TalkServerConfiguration.class);

    public static final int THREADS_DELIVERY = 1;
    public static final int THREADS_UPDATE = 1;
    public static final int THREADS_PUSH = 1;
    public static final int THREADS_PING = 2; // XXX HIGHER COUNT?
    public static final int THREADS_CLEANING = 4;

    public static final int PING_INTERVAL = 300; // in seconds
    public static final boolean PERFORM_PING_AT_INTERVALS = false;

    // top level property prefix for all talk related properties, e.g. 'talk.foo.bar'
    private static final String PROPERTY_PREFIX = "talk";

    private static final String PROPERTY_LISTEN_ADDRESS = PROPERTY_PREFIX + ".listen.address";
    private String mListenAddress = "localhost";

    private static final String PROPERTY_LISTEN_PORT = PROPERTY_PREFIX + ".listen.port";
    private int    mListenPort = 8080;

    private int     mPushRateLimit = 15000;
    private boolean mGcmEnabled = false;
    private String  mGcmApiKey = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";
    private int     mGcmWakeTtl = 1 * 7 * 24 * 3600; // 1 week

    // APNS settings
    private boolean mApnsEnabled = false;
    private String  mApnsCertProductionPath = "apns_production.p12";
    private String  mApnsCertProductionPassword = "password";
    private String  mApnsCertSandboxPath =  "apns_sandbox.p12";
    private String  mApnsCertSandboxPassword = "password";
    private int     mApnsInvalidateDelay = 30;
    private int     mApnsInvalidateInterval = 3600;

    private String mDatabaseBackend = "jongo";
    private String mJongoDb = "talk";

    private int mCleanupAllClientsDelay = 7200; // 2 hours //300;
    private int mCleanupAllClientsInterval = 60 * 60 * 24; // once a day //900;
    private int mCleanupAllDeliveriesDelay = 3600; // 1 hour //600;
    private int mCleanupAllDeliveriesInterval = 60 * 60 * 6; // every 6 hours //900;


    private String mFilecacheControlUrl = "http://localhost:8081/control";
    private String mFilecacheUploadBase = "http://localhost:8081/upload/";
    private String mFilecacheDownloadBase = "http://localhost:8081/download/";

    private String mSupportTag = "Oos8guceich2yoox";

    private boolean mLogAllCalls = false;

    private String mVersion = "<unknown>";
    private String mBuildNumber;
    private GitInfo gitInfo = new GitInfo();

    public TalkServerConfiguration() {
    }

    public void report() {
        LOG.info("Current configuration:" +
                        "\n - General:" +
                        MessageFormat.format("\n   * version:                            ''{0}''", mVersion) +
                        MessageFormat.format("\n   * git.commit.id:                      ''{0}''", gitInfo.commitId) +
                        MessageFormat.format("\n   * git.commit.id.abbrev:               ''{0}''", gitInfo.commitIdAbbrev) +
                        MessageFormat.format("\n   * git.branch:                         ''{0}''", gitInfo.branch) +
                        MessageFormat.format("\n   * git.commit.time:                    ''{0}''", gitInfo.commitTime) +
                        MessageFormat.format("\n   * git.build.time:                     ''{0}''", gitInfo.buildTime) +
                        "\n - WebServer Configuration:" +
                        MessageFormat.format("\n   * listen address:                     ''{0}''", mListenAddress) +
                        MessageFormat.format("\n   * listen port:                        ''{0}''", Long.toString(mListenPort)) +
                        "\n - Database Configuration:" +
                        MessageFormat.format("\n   * database backend:                   ''{0}''", mDatabaseBackend) +
                        MessageFormat.format("\n   * jongo database:                     ''{0}''", mJongoDb) +
                        "\n - Push Configuration:" +
                        MessageFormat.format("\n   * push rate limit:                    ''{0}''", Long.toString(mPushRateLimit)) +
                        "\n   - APNS:" +
                        MessageFormat.format("\n     * enabled:                          ''{0}''", mApnsEnabled) +
                        MessageFormat.format("\n     * production cert path :            ''{0}''", mApnsCertProductionPath) +
                        MessageFormat.format("\n     * production cert password (length):''{0}''", mApnsCertProductionPassword.length()) + // here we don't really print the password literal to stdout of course
                        MessageFormat.format("\n     * sandbox cert path :               ''{0}''", mApnsCertSandboxPath) +
                        MessageFormat.format("\n     * sandbox cert password (length):   ''{0}''", mApnsCertSandboxPassword.length()) + // here we don't really print the password literal to stdout of course

                        MessageFormat.format("\n     * apns invalidate delay:            ''{0}''", mApnsInvalidateDelay) +
                        MessageFormat.format("\n     * apns invalidate interval:         ''{0}''", mApnsInvalidateInterval) +
                        "\n   - GCM:" +
                        MessageFormat.format("\n     * gcm enabled:                      ''{0}''", mGcmEnabled) +
                        MessageFormat.format("\n     * gcm api key (length):             ''{0}''", mGcmApiKey.length()) +
                        "\n - Cleaning Agent Configuration:" +
                        MessageFormat.format("\n   * clients cleanup delay (in s):       ''{0}''", Long.toString(mCleanupAllClientsDelay)) +
                        MessageFormat.format("\n   * clients cleanup interval (in s):    ''{0}''", Long.toString(mCleanupAllClientsInterval)) +
                        MessageFormat.format("\n   * deliveries cleanup delay (in s):    ''{0}''", Long.toString(mCleanupAllDeliveriesDelay)) +
                        MessageFormat.format("\n   * deliveries cleanup interval (in s): ''{0}''", Long.toString(mCleanupAllDeliveriesInterval)) +
                        "\n - Filecache Configuration:" +
                        MessageFormat.format("\n   * filecache control url:              ''{0}''", mFilecacheControlUrl) +
                        MessageFormat.format("\n   * filecache upload base url:          ''{0}''", mFilecacheUploadBase) +
                        MessageFormat.format("\n   * filecache download base url:        ''{0}''", mFilecacheDownloadBase) +
                        "\n - Other:" +
                        MessageFormat.format("\n   * support tag: ''{0}''", mSupportTag) +
                        "\n - Constants:" +
                        MessageFormat.format("\n   * DeliveryAgent Threads Poolsize:     ''{0}''", THREADS_DELIVERY) +
                        MessageFormat.format("\n   * CleanupAgent  Threads Poolsize:     ''{0}''", THREADS_CLEANING) +
                        MessageFormat.format("\n   * PushAgent     Threads Poolsize:     ''{0}''", THREADS_PUSH) +
                        MessageFormat.format("\n   * PingAgent     Threads Poolsize:     ''{0}''", THREADS_PING) +
                        MessageFormat.format("\n   * UpdateAgent   Threads Poolsize:     ''{0}''", THREADS_UPDATE) +
                        MessageFormat.format("\n   * Ping interval (in s):               ''{0}''", PING_INTERVAL) +
                        MessageFormat.format("\n   * perform ping at intervals:          ''{0}''", PERFORM_PING_AT_INTERVALS) +
                        "\n - Debugging:" +
                        MessageFormat.format("\n   * LogAllCalls:     ''{0}''", mLogAllCalls)
        );
    }

    public void configureFromProperties(Properties properties) {
        // listening
        mListenAddress = properties.getProperty(PROPERTY_LISTEN_ADDRESS, mListenAddress);
        mListenPort = Integer.parseInt(properties.getProperty(PROPERTY_LISTEN_PORT, Integer.toString(mListenPort)));

        // Database
        mDatabaseBackend = properties.getProperty(PROPERTY_PREFIX + ".db.backend", mDatabaseBackend);

        // Jongo
        mJongoDb = properties.getProperty(PROPERTY_PREFIX + ".jongo.db", mJongoDb);

        // Push
        mPushRateLimit = Integer.valueOf(properties.getProperty(PROPERTY_PREFIX + ".push.rateLimit", Integer.toString(mPushRateLimit)));

        // APNS
        mApnsEnabled = Boolean.valueOf(properties.getProperty(PROPERTY_PREFIX + ".apns.enabled", Boolean.toString(mApnsEnabled)));
        mApnsCertProductionPath = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.production.path", mApnsCertProductionPath);
        mApnsCertProductionPassword = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.production.password", mApnsCertProductionPassword);
        mApnsCertSandboxPath = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.sandbox.path", mApnsCertSandboxPath);
        mApnsCertSandboxPassword = properties.getProperty(PROPERTY_PREFIX + ".apns.cert.sandbox.password", mApnsCertSandboxPassword);

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

        // Debugging
        mLogAllCalls = Boolean.valueOf(properties.getProperty(PROPERTY_PREFIX + ".debug.logallcalls", Boolean.toString(mLogAllCalls)));
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

    public String getApnsCertProductionPath() {
        return mApnsCertProductionPath;
    }

    public String getApnsCertProductionPassword() {
        return mApnsCertProductionPassword;
    }

    public String getApnsCertSandboxPath() {
        return mApnsCertSandboxPath;
    }

    public String getApnsCertSandboxPassword() {
        return mApnsCertSandboxPassword;
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

    public void setFilecacheControlUrl(String mFilecacheControlUrl) {
        this.mFilecacheControlUrl = mFilecacheControlUrl;
    }

    public void setFilecacheUploadBase(String mFilecacheUploadBase) {
        this.mFilecacheUploadBase = mFilecacheUploadBase;
    }

    public void setFilecacheDownloadBase(String mFilecacheDownloadBase) {
        this.mFilecacheDownloadBase = mFilecacheDownloadBase;
    }

    public String getSupportTag() {
        return mSupportTag;
    }

    public void setLogAllCalls(boolean flag) {
        mLogAllCalls = flag;
    }

    public boolean getLogAllCalls() {
        return mLogAllCalls;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        this.mVersion = version;
    }

    public void setBuildNumber(String buildNumber) {
        this.mBuildNumber = buildNumber;
    }

    public String getBuildNumber() {
        return mBuildNumber;
    }

    public void setGitInfo(GitInfo gitInfo) {
        this.gitInfo = gitInfo;
    }

    public GitInfo getGitInfo() {
        return gitInfo;
    }
}
