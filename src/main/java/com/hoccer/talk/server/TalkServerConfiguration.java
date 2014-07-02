package com.hoccer.talk.server;

import com.hoccer.scm.GitInfo;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Encapsulation of server configuration
 * <p/>
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

    public final static int GCM_WAKE_TTL = 1 * 7 * 24 * 3600; // 1 week

    // top level property prefix for all talk related properties, e.g. 'talk.foo.bar'
    private static final String PROPERTY_PREFIX = "talk";

    private enum PropertyTypes {STRING, BOOLEAN, INTEGER}

    private enum ConfigurableProperties {
        // WEB-SERVER
        LISTEN_ADDRESS(PROPERTY_PREFIX + ".listen.address",
                PropertyTypes.STRING,
                "localhost"),
        LISTEN_PORT(PROPERTY_PREFIX + ".listen.port",
                PropertyTypes.INTEGER,
                8080),
        // DATABASE
        DATABASE_BACKEND(PROPERTY_PREFIX + ".db.backend",
                PropertyTypes.STRING,
                "jongo"),
        JONGO_HOST(PROPERTY_PREFIX + ".jongo.host",
                PropertyTypes.STRING,
                "localhost"),
        JONGO_DATABASE(PROPERTY_PREFIX + ".jongo.db",
                PropertyTypes.STRING,
                "talk"),
        JONGO_CONNECTIONS_PER_HOST(PROPERTY_PREFIX + ".jongo.connectionsPerHost",
                PropertyTypes.INTEGER,
                10),
        JONGO_MAX_WAIT_TIME(PROPERTY_PREFIX + ".jongo.maxWaitTime",
                PropertyTypes.INTEGER,
                5 * 1000), // in milliseconds (5 seconds)
        // PUSH GENERIC
        PUSH_RATE_LIMIT(PROPERTY_PREFIX + ".push.rateLimit",
                PropertyTypes.INTEGER,
                15000),
        // APNS
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
        // GCM
        GCM_ENABLED(PROPERTY_PREFIX + ".gcm.enabled",
                PropertyTypes.BOOLEAN,
                false),
        GCM_API_KEY(PROPERTY_PREFIX + ".gcm.apikey",
                PropertyTypes.STRING,
                "AIzaSyA25wabV4kSQTaF73LTgTkjmw0yZ8inVr8"), // TODO: Do we really need this api key in code here?
        // CLEANUP
        CLEANUP_ALL_CLIENTS_DELAY(PROPERTY_PREFIX + ".cleanup.allClientsDelay",
                PropertyTypes.INTEGER,
                7200), // in seconds (2 hours)
        CLEANUP_ALL_CLIENTS_INTERVAL(PROPERTY_PREFIX + ".cleanup.allClientsInterval",
                PropertyTypes.INTEGER,
                60 * 60 * 24), // in seconds (once a day)
        CLEANUP_ALL_DEVLIVERIES_DELAY(PROPERTY_PREFIX + ".cleanup.allDeliveriesDelay",
                PropertyTypes.INTEGER,
                3600), // in second (1 hour)
        CLEANUP_ALL_DELIVERIES_INTERVAL(PROPERTY_PREFIX + ".cleanup.allDeliveriesInterval",
                PropertyTypes.INTEGER,
                60 * 60 * 6), // in seconds (every 6 hours)
        // FILECACHE
        FILECACHE_CONTROL_URL(PROPERTY_PREFIX + ".filecache.controlUrl",
                PropertyTypes.STRING,
                "http://localhost:8081/control"),
        FILECACHE_UPLOAD_BASE(PROPERTY_PREFIX + ".filecache.uploadBase",
                PropertyTypes.STRING,
                "http://localhost:8081/upload/"),
        FILECACHE_DOWNLOAD_BASE(PROPERTY_PREFIX + ".filecache.downloadBase",
                PropertyTypes.STRING,
                "http://localhost:8081/download/"),
        // MISC
        SUPPORT_TAG(PROPERTY_PREFIX + ".support.tag",
                PropertyTypes.STRING,
                "Oos8guceich2yoox"),
        LOG_ALL_CALLS(PROPERTY_PREFIX + ".debug.logallcalls",
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
            for (ConfigurableProperties property : ConfigurableProperties.values()) {
                LOG.debug("Loading configurable property " + property.name() + " from key: '" + property.key + "'");
                String rawValue = properties.getProperty(property.key);
                if (rawValue != null) {
                    property.setValue(rawValue);
                } else {
                    LOG.info("Property " + property.name() + " (type: " + property.type.name() + ") is unconfigured (key: '" + property.key + "'): default value is '" + property.value + "'");
                }
            }
        }

        public void setValue(String rawValue) {
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

    private String mVersion = "<unknown>";
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
                        MessageFormat.format("\n   * listen port:                        {0}", Long.toString(getListenPort())) +
                        "\n - Database Configuration:" +
                        MessageFormat.format("\n   * database backend:                   ''{0}''", this.getDatabaseBackend()) +
                        MessageFormat.format("\n   * jongo host:                         ''{0}''", this.getJongoHost()) +
                        MessageFormat.format("\n   * jongo database:                     ''{0}''", this.getJongoDb()) +
                        MessageFormat.format("\n   * jongo connections/host:             ''{0}''", this.getJongoConnectionsPerHost()) +
                        MessageFormat.format("\n   * jongo max wait time:                ''{0}''", this.getJongoMaxWaitTime()) +
                        "\n - Push Configuration:" +
                        MessageFormat.format("\n   * push rate limit (in milli-seconds): {0}", Long.toString(this.getPushRateLimit())) +
                        "\n   - APNS:" +
                        MessageFormat.format("\n     * enabled:                          {0}", this.isApnsEnabled()) +
                        MessageFormat.format("\n     * production cert path :            ''{0}''", this.getApnsCertProductionPath()) +
                        MessageFormat.format("\n     * production cert password (length):''{0}''", this.getApnsCertProductionPassword().length()) + // here we don't really print the password literal to stdout of course
                        MessageFormat.format("\n     * sandbox cert path :               ''{0}''", this.getApnsCertSandboxPath()) +
                        MessageFormat.format("\n     * sandbox cert password (length):   ''{0}''", this.getApnsCertSandboxPassword().length()) + // here we don't really print the password literal to stdout of course
                        MessageFormat.format("\n     * apns invalidate delay (in s):     {0}", Long.toString(this.getApnsInvalidateDelay())) +
                        MessageFormat.format("\n     * apns invalidate interval (in s):  {0}", Long.toString(this.getApnsInvalidateInterval())) +
                        "\n   - GCM:" +
                        MessageFormat.format("\n     * enabled:                          {0}", this.isGcmEnabled()) +
                        MessageFormat.format("\n     * api key (length):                 ''{0}''", getGcmApiKey().length()) +
                        MessageFormat.format("\n     * wake ttl (in s)                   ''{0}''", Long.toString(GCM_WAKE_TTL)) +
                        "\n - Cleaning Agent Configuration:" +
                        MessageFormat.format("\n   * clients cleanup delay (in s):       {0}", Long.toString(this.getApnsInvalidateDelay())) +
                        MessageFormat.format("\n   * clients cleanup interval (in s):    {0}", Long.toString(this.getCleanupAllClientsInterval())) +
                        MessageFormat.format("\n   * deliveries cleanup delay (in s):    {0}", Long.toString(this.getCleanupAllDeliveriesDelay())) +
                        MessageFormat.format("\n   * deliveries cleanup interval (in s): {0}", Long.toString(this.getCleanupAllDeliveriesInterval())) +
                        "\n - Filecache Configuration:" +
                        MessageFormat.format("\n   * filecache control url:              ''{0}''", this.getFilecacheControlUrl()) +
                        MessageFormat.format("\n   * filecache upload base url:          ''{0}''", this.getFilecacheUploadBase()) +
                        MessageFormat.format("\n   * filecache download base url:        ''{0}''", this.getFilecacheDownloadBase()) +
                        "\n - Other:" +
                        MessageFormat.format("\n   * support tag: ''{0}''", this.getSupportTag()) +
                        "\n - Threads:" +
                        MessageFormat.format("\n   * DeliveryAgent Threads Poolsize:     {0}", THREADS_DELIVERY) +
                        MessageFormat.format("\n   * CleanupAgent  Threads Poolsize:     {0}", THREADS_CLEANING) +
                        MessageFormat.format("\n   * PushAgent     Threads Poolsize:     {0}", THREADS_PUSH) +
                        MessageFormat.format("\n   * PingAgent     Threads Poolsize:     {0}", THREADS_PING) +
                        MessageFormat.format("\n   * UpdateAgent   Threads Poolsize:     {0}", THREADS_UPDATE) +
                        "\n - Ping:" +
                        MessageFormat.format("\n   * Ping interval (in s):               {0}", PING_INTERVAL) +
                        MessageFormat.format("\n   * perform ping at intervals:          {0}", PERFORM_PING_AT_INTERVALS) +
                        "\n - Debugging:" +
                        MessageFormat.format("\n   * LogAllCalls:                        {0}", this.getLogAllCalls())
        );
    }

    public void configureFromProperties(Properties properties) {
        LOG.info("Loading from properties...");
        ConfigurableProperties.loadFromProperties(properties);
    }

    public String getListenAddress() {
        return (String) ConfigurableProperties.LISTEN_ADDRESS.value;
    }

    public int getListenPort() {
        return (Integer) ConfigurableProperties.LISTEN_PORT.value;
    }

    public String getDatabaseBackend() {
        return (String) ConfigurableProperties.DATABASE_BACKEND.value;
    }

    public String getJongoDb() {
        return (String) ConfigurableProperties.JONGO_DATABASE.value;
    }

    public int getJongoConnectionsPerHost() {
        return (Integer) ConfigurableProperties.JONGO_CONNECTIONS_PER_HOST.value;
    }

    public int getJongoMaxWaitTime() {
        return (Integer) ConfigurableProperties.JONGO_MAX_WAIT_TIME.value;
    }

    public String getJongoHost() {
        return (String) ConfigurableProperties.JONGO_HOST.value;
    }

    public int getPushRateLimit() {
        return (Integer) ConfigurableProperties.PUSH_RATE_LIMIT.value;
    }

    public boolean isGcmEnabled() {
        return (Boolean) ConfigurableProperties.GCM_ENABLED.value;
    }

    public String getGcmApiKey() {
        return (String) ConfigurableProperties.GCM_API_KEY.value;
    }

    public boolean isApnsEnabled() {
        return (Boolean) ConfigurableProperties.APNS_ENABLED.value;
    }

    public String getApnsCertProductionPath() {
        return (String) ConfigurableProperties.APNS_PRODUCTION_CERTIFICATE_PATH.value;
    }

    public String getApnsCertProductionPassword() {
        return (String) ConfigurableProperties.APNS_PRODUCTION_CERTIFICATE_PASSWORD.value;
    }

    public String getApnsCertSandboxPath() {
        return (String) ConfigurableProperties.APNS_SANDBOX_CERTIFICATE_PATH.value;
    }

    public String getApnsCertSandboxPassword() {
        return (String) ConfigurableProperties.APNS_SANDBOX_CERTIFICATE_PASSWORD.value;
    }

    public int getApnsInvalidateDelay() {
        return (Integer) ConfigurableProperties.APNS_INVALIDATE_DELAY.value;
    }

    public int getApnsInvalidateInterval() {
        return (Integer) ConfigurableProperties.APNS_INVALIDATE_INTERVAL.value;
    }

    public int getCleanupAllClientsDelay() {
        return (Integer) ConfigurableProperties.CLEANUP_ALL_CLIENTS_DELAY.value;
    }

    public int getCleanupAllClientsInterval() {
        return (Integer) ConfigurableProperties.CLEANUP_ALL_CLIENTS_INTERVAL.value;
    }

    public int getCleanupAllDeliveriesDelay() {
        return (Integer) ConfigurableProperties.CLEANUP_ALL_DEVLIVERIES_DELAY.value;
    }

    public int getCleanupAllDeliveriesInterval() {
        return (Integer) ConfigurableProperties.CLEANUP_ALL_CLIENTS_INTERVAL.value;
    }

    public URI getFilecacheControlUrl() {
        URI url = null;
        try {
            url = new URI((String) ConfigurableProperties.FILECACHE_CONTROL_URL.value);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return url;
    }

    public void setFilecacheControlUrl(String pFilecacheControlUrl) {
        ConfigurableProperties.FILECACHE_CONTROL_URL.setValue(pFilecacheControlUrl);
    }

    public String getFilecacheUploadBase() {
        return (String) ConfigurableProperties.FILECACHE_UPLOAD_BASE.value;
    }

    public void setFilecacheUploadBase(String pFilecacheUploadBase) {
        ConfigurableProperties.FILECACHE_UPLOAD_BASE.setValue(pFilecacheUploadBase);
    }

    public String getFilecacheDownloadBase() {
        return (String) ConfigurableProperties.FILECACHE_DOWNLOAD_BASE.value;
    }

    public void setFilecacheDownloadBase(String mFilecacheDownloadBase) {
        ConfigurableProperties.FILECACHE_DOWNLOAD_BASE.setValue(mFilecacheDownloadBase);
    }

    public String getSupportTag() {
        return (String) ConfigurableProperties.SUPPORT_TAG.value;
    }

    public boolean getLogAllCalls() {
        return (Boolean) ConfigurableProperties.LOG_ALL_CALLS.value;
    }

    public void setLogAllCalls(Boolean flag) {
        ConfigurableProperties.LOG_ALL_CALLS.setValue(flag.toString());
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        this.mVersion = version;
    }

    public void setGitInfo(GitInfo gitInfo) {
        this.gitInfo = gitInfo;
    }

    public GitInfo getGitInfo() {
        return gitInfo;
    }
}
