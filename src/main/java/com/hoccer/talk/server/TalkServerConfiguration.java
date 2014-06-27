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
    private enum PropertyTypes {STRING, BOOLEAN, INTEGER}
    private enum ConfigurableProperties {
        LISTEN_ADDRESS(PROPERTY_PREFIX + ".listen.address",
                PropertyTypes.STRING,
                "localhost"),
        LISTEN_PORT(PROPERTY_PREFIX + ".listen.port",
                PropertyTypes.INTEGER,
                8080),
        DATABASE_BACKEND(PROPERTY_PREFIX + ".db.backend",
                PropertyTypes.STRING,
                "jongo"),
        JONGO_DATABASE(PROPERTY_PREFIX + ".jongo.db",
                PropertyTypes.STRING,
                "talk"),
        PUSH_RATE_LIMIT(PROPERTY_PREFIX + ".push.rateLimit",
                PropertyTypes.INTEGER,
                15000),
        APNS_ENABLED(PROPERTY_PREFIX + ".apns.enabled",
                PropertyTypes.BOOLEAN,
                false),
        APNS_PRODUCTION_CERTIFICATE_PATH(PROPERTY_PREFIX + ".apns.cert.production.path",
                PropertyTypes.STRING,
                "apns_production.p12"),
        APNS_PRODUCTION_CERTIFICATE_PASSWORD(PROPERTY_PREFIX + ".apns.cert.production.password",
                PropertyTypes.STRING,
                "password"),
        APNS_SANDBOX_CERTIFICATE_PATH(PROPERTY_PREFIX + ".apns.cert.sandbox.path",
                PropertyTypes.STRING,
                "apns_sandbox.p12"),
        APNS_SANDBOX_CERTIFICATE_PASSWORD(PROPERTY_PREFIX + ".apns.cert.sandbox.password",
                PropertyTypes.STRING,
                "password"),
        APNS_INVALIDATE_DELAY(PROPERTY_PREFIX + ".apns.invalidate.delay",
                PropertyTypes.INTEGER,
                30), // in seconds
        APNS_INVALIDATE_INTERVAL(PROPERTY_PREFIX + ".apns.invalidate.interval",
                PropertyTypes.INTEGER,
                3600), // in seconds
        GCM_ENABLED(PROPERTY_PREFIX + "gcm.enabled",
                PropertyTypes.BOOLEAN,
                false);

        public final String key;
        public final PropertyTypes type;
        public Object value;

        private ConfigurableProperties(String name, PropertyTypes type, Object defaultValue) {
            this.key = name;
            this.type = type;
            this.value = defaultValue;
        }

        static public void loadFromProperties(Properties properties) {
            for(ConfigurableProperties property : ConfigurableProperties.values()) {
                LOG.debug("Loading configurable property " + property.name() + " from key: '" + property.key + "'");
                String rawValue = properties.getProperty(property.key);
                if (rawValue != null) {
                    property.setValue(rawValue);
                } else {
                    LOG.info("Property " + property.name() + " (type: " + property.type.name() + ") is unconfigured (key: '" + property.key + "'): default value is '" + property.value + "'");
                }
            }
        }

        private void setValue(String rawValue) {
            LOG.debug("   - setValue: " + rawValue + "  (" + this.type.name() + ")");
            if (PropertyTypes.STRING.equals(this.type)) {
                this.value = rawValue;
            } else if (PropertyTypes.INTEGER.equals(this.type)) {
                this.value = Integer.valueOf(rawValue);
            } else if (PropertyTypes.BOOLEAN.equals(this.type)) {
                this.value = Boolean.valueOf(rawValue);
            }
        }
    }

    private String  mGcmApiKey = "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8";
    private int     mGcmWakeTtl = 1 * 7 * 24 * 3600; // 1 week

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
                        MessageFormat.format("\n   * listen address:                     ''{0}''", this.getListenAddress()) +
                        MessageFormat.format("\n   * listen port:                        {0}",     Long.toString(getListenPort())) +
                        "\n - Database Configuration:" +
                        MessageFormat.format("\n   * database backend:                   ''{0}''", this.getDatabaseBackend()) +
                        MessageFormat.format("\n   * jongo database:                     ''{0}''", this.getJongoDb()) +
                        "\n - Push Configuration:" +
                        MessageFormat.format("\n   * push rate limit (in milli-seconds): {0}",     Long.toString(this.getPushRateLimit())) +
                        "\n   - APNS:" +
                        MessageFormat.format("\n     * enabled:                          ''{0}''", this.isApnsEnabled()) +
                        MessageFormat.format("\n     * production cert path :            ''{0}''", this.getApnsCertProductionPath()) +
                        MessageFormat.format("\n     * production cert password (length):''{0}''", this.getApnsCertProductionPassword().length()) + // here we don't really print the password literal to stdout of course
                        MessageFormat.format("\n     * sandbox cert path :               ''{0}''", this.getApnsCertSandboxPath()) +
                        MessageFormat.format("\n     * sandbox cert password (length):   ''{0}''", this.getApnsCertSandboxPassword().length()) + // here we don't really print the password literal to stdout of course
                        MessageFormat.format("\n     * apns invalidate delay (in s):     {0}", Long.toString(this.getApnsInvalidateDelay())) +
                        MessageFormat.format("\n     * apns invalidate interval (in s):  {0}", Long.toString(this.getApnsInvalidateInterval())) +
                        "\n   - GCM:" +
                        MessageFormat.format("\n     * gcm enabled:                      ''{0}''", this.isGcmEnabled()) +
                        MessageFormat.format("\n     * gcm api key (length):             ''{0}''", mGcmApiKey.length()) +
                        "\n - Cleaning Agent Configuration:" +
                        MessageFormat.format("\n   * clients cleanup delay (in s):       {0}",     Long.toString(mCleanupAllClientsDelay)) +
                        MessageFormat.format("\n   * clients cleanup interval (in s):    {0}",     Long.toString(mCleanupAllClientsInterval)) +
                        MessageFormat.format("\n   * deliveries cleanup delay (in s):    {0}",     Long.toString(mCleanupAllDeliveriesDelay)) +
                        MessageFormat.format("\n   * deliveries cleanup interval (in s): {0}",     Long.toString(mCleanupAllDeliveriesInterval)) +
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
        LOG.info("Loading from properties...");
        ConfigurableProperties.loadFromProperties(properties);

        // GCM
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
        return (String)ConfigurableProperties.LISTEN_ADDRESS.value;
    }

    public int getListenPort() {
        return (Integer)ConfigurableProperties.LISTEN_PORT.value;
    }

    public String getDatabaseBackend() {
        return (String)ConfigurableProperties.DATABASE_BACKEND.value;
    }

    public String getJongoDb() {
        return (String)ConfigurableProperties.JONGO_DATABASE.value;
    }

    public int getPushRateLimit() {
        return (Integer)ConfigurableProperties.PUSH_RATE_LIMIT.value;
    }

    public boolean isGcmEnabled() {
        return (Boolean)ConfigurableProperties.GCM_ENABLED.value;
    }

    public String getGcmApiKey() {
        return mGcmApiKey;
    }

    public int getGcmWakeTtl() {
        return mGcmWakeTtl;
    }

    public boolean isApnsEnabled() {
        return (Boolean)ConfigurableProperties.APNS_ENABLED.value;
    }

    public String getApnsCertProductionPath() {
        return (String)ConfigurableProperties.APNS_PRODUCTION_CERTIFICATE_PATH.value;
    }

    public String getApnsCertProductionPassword() {
        return (String)ConfigurableProperties.APNS_PRODUCTION_CERTIFICATE_PASSWORD.value;
    }

    public String getApnsCertSandboxPath() {
        return (String)ConfigurableProperties.APNS_SANDBOX_CERTIFICATE_PATH.value;
    }

    public String getApnsCertSandboxPassword() {
        return (String)ConfigurableProperties.APNS_SANDBOX_CERTIFICATE_PASSWORD.value;
    }

    public int getApnsInvalidateDelay() {
        return (Integer)ConfigurableProperties.APNS_INVALIDATE_DELAY.value;
    }

    public int getApnsInvalidateInterval() {
        return (Integer)ConfigurableProperties.APNS_INVALIDATE_INTERVAL.value;
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
