package com.hoccer.talk.server;

import better.jsonrpc.server.JsonRpcServer;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkEnvironment;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.cleaning.CleaningAgent;
import com.hoccer.talk.server.database.DatabaseHealthCheck;
import com.hoccer.talk.server.delivery.DeliveryAgent;
import com.hoccer.talk.server.filecache.FilecacheClient;
import com.hoccer.talk.server.ping.PingAgent;
import com.hoccer.talk.server.push.PushAgent;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import com.hoccer.talk.server.update.UpdateAgent;
import de.undercouch.bson4jackson.BsonFactory;

import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main object of the Talk server
 * <p/>
 * This holds global state such as the list of active connections,
 * references to common database mapping helpers and so on.
 */
public class TalkServer {

    /**
     * server-global JSON mapper
     */
    ObjectMapper mJsonMapper;

    /**
     * server-global BSON mapper
     */
    ObjectMapper mBsonMapper;

    /**
     * Metrics registry
     */
    MetricRegistry mMetricsRegistry;
    HealthCheckRegistry mHealthRegistry;
    JmxReporter mJmxReporter;

    /**
     * JSON-RPC server instance
     */
    JsonRpcServer mRpcServer;

    /**
     * Server configuration
     */
    TalkServerConfiguration mConfiguration;

    /**
     * Database accessor
     */
    ITalkServerDatabase mDatabase;

    /**
     * Stats collector
     */
    ITalkServerStatistics mStatistics;

    /**
     * Delivery agent
     */
    DeliveryAgent mDeliveryAgent;

    /**
     * Push service agent
     */
    PushAgent mPushAgent;

    /**
     * Presence update agent
     */
    UpdateAgent mUpdateAgent;

    /**
     * Ping measurement agent
     */
    PingAgent mPingAgent;

    /**
     * Cleaning agent
     */
    CleaningAgent mCleaningAgent;

    /**
     * Client for the filecache control interface
     */
    FilecacheClient mFilecacheClient;

    /**
     * All connections (every connected websocket)
     */
    Vector<TalkRpcConnection> mConnections =
            new Vector<TalkRpcConnection>();

    /**
     * All logged-in connections by client ID
     */
    Hashtable<String, TalkRpcConnection> mConnectionsByClientId =
            new Hashtable<String, TalkRpcConnection>();

    AtomicInteger mConnectionsTotal = new AtomicInteger();
    AtomicInteger mConnectionsOpen = new AtomicInteger();

    /**
     * Create and initialize a Hoccer Talk server
     */
    public TalkServer(TalkServerConfiguration configuration, ITalkServerDatabase database) {
        mConfiguration = configuration;
        mDatabase = database;

        mJsonMapper = createObjectMapper(new JsonFactory());
        mBsonMapper = createObjectMapper(new BsonFactory());

        mMetricsRegistry = new MetricRegistry();
        initializeMetrics();
        mHealthRegistry = new HealthCheckRegistry();
        initializeHealthChecks();
        mStatistics = new TalkMetricStats(mMetricsRegistry);

        mRpcServer = new JsonRpcServer(ITalkRpcServer.class);
        mDeliveryAgent = new DeliveryAgent(this);
        mPushAgent = new PushAgent(this);
        mUpdateAgent = new UpdateAgent(this);
        mPingAgent = new PingAgent(this);
        mCleaningAgent = new CleaningAgent(this);
        mFilecacheClient = new FilecacheClient(this);

        // For instrumenting metrics via JMX
        mJmxReporter = JmxReporter.forRegistry(mMetricsRegistry).build();
        mJmxReporter.start();
    }

    /**
     * @return the JSON mapper used by this server
     */
    public ObjectMapper getJsonMapper() {
        return mJsonMapper;
    }

    /**
     * @return the BSON mapper used by this server
     */
    public ObjectMapper getBsonMapper() {
        return mBsonMapper;
    }

    /**
     * @return the metrics registry for the server
     */
    public MetricRegistry getMetrics() {
        return mMetricsRegistry;
    }

    /**
     * @return the JSON-RPC server
     */
    public JsonRpcServer getRpcServer() {
        return mRpcServer;
    }

    /**
     * @return the configuration of this server
     */
    public TalkServerConfiguration getConfiguration() {
        return mConfiguration;
    }

    /**
     * @return the database accessor of this server
     */
    public ITalkServerDatabase getDatabase() {
        return mDatabase;
    }

    /**
     * @return the stats collector for this server
     */
    public ITalkServerStatistics getStatistics() {
        return mStatistics;
    }

    /**
     * @return the push agent of this server
     */
    public PushAgent getPushAgent() {
        return mPushAgent;
    }

    /**
     * @return the delivery agent of this server
     */
    public DeliveryAgent getDeliveryAgent() {
        return mDeliveryAgent;
    }

    /**
     * @return the update agent of this server
     */
    public UpdateAgent getUpdateAgent() {
        return mUpdateAgent;
    }

    /**
     * @return the ping agent of this server
     */
    public PingAgent getPingAgent() {
        return mPingAgent;
    }

    /**
     * @return the cleaning agent
     */
    public CleaningAgent getCleaningAgent() {
        return mCleaningAgent;
    }

    /**
     * @return the filecache control client
     */
    public FilecacheClient getFilecacheClient() {
        return mFilecacheClient;
    }

    /**
     * Check if the given client is connected
     *
     * @param clientId of the client to check for
     * @return true if the client is connected
     */
    public boolean isClientConnected(String clientId) {
        return getClientConnection(clientId) != null;
    }

    /**
     * Retrieve the connection of the given client
     *
     * @param clientId of the client to check for
     * @return connection of the client or null
     */
    public TalkRpcConnection getClientConnection(String clientId) {
        return mConnectionsByClientId.get(clientId);
    }

    /**
     * Notify the server of a successful login
     *
     * @param client     that was logged in
     * @param connection the client is on
     */
    public void identifyClient(TalkClient client, TalkRpcConnection connection) {
        String clientId = client.getClientId();
        TalkRpcConnection oldConnection = mConnectionsByClientId.get(clientId);
        if (oldConnection != null) {
            // TODO: LOG this - maybe even on warn level!
            oldConnection.disconnect();
        }
        connection.getServerHandler().destroyEnvironment(TalkEnvironment.TYPE_NEARBY);  // after logon, destroy possibly left over environments
        mConnectionsByClientId.put(clientId, connection);
    }

    /**
     * Notify the server of a ready call
     *
     * @param client     that called ready
     * @param connection the client is on
     */
    public void readyClient(TalkClient client, TalkRpcConnection connection) {
        mUpdateAgent.requestPresenceUpdate(client.getClientId());
    }

    /**
     * Register a new connection with the server
     *
     * @param connection to be registered
     */
    public void connectionOpened(TalkRpcConnection connection) {
        mConnectionsTotal.incrementAndGet();
        mConnectionsOpen.incrementAndGet();
        mConnections.add(connection);
    }

    /**
     * Unregister a connection from the server
     *
     * @param connection to be removed
     */
    public void connectionClosed(TalkRpcConnection connection) {
        mConnectionsOpen.decrementAndGet();
        // remove connection from list
        mConnections.remove(connection);
        // remove connection from table
        if (connection.getClientId() != null) {
            connection.getServerHandler().destroyEnvironment(TalkEnvironment.TYPE_NEARBY);

            String clientId = connection.getClientId();
            // remove connection from table
            mConnectionsByClientId.remove(clientId);
            // update presence for connection status change
            mUpdateAgent.requestPresenceUpdate(clientId);
        }
        // disconnect if we still are
        if (connection.isConnected()) {
            connection.disconnect();
        }
    }

    /**
     * Creates the object mapper for this server
     */
    private ObjectMapper createObjectMapper(JsonFactory factory) {
        ObjectMapper result = new ObjectMapper(factory);
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        result.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return result;
    }

    /**
     * Set up server metrics
     */
    private void initializeMetrics() {
        mMetricsRegistry.register(MetricRegistry.name(TalkServer.class, "connectionsOpen"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mConnectionsOpen.intValue();
                    }
                }
        );
        mMetricsRegistry.register(MetricRegistry.name(TalkServer.class, "connectionsTotal"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mConnectionsTotal.intValue();
                    }
                }
        );
        // For instrumenting JMX via Metrics
        mMetricsRegistry.register(MetricRegistry.name("jvm", "gc"), new GarbageCollectorMetricSet());
        mMetricsRegistry.register(MetricRegistry.name("jvm", "memory"), new MemoryUsageGaugeSet());
        mMetricsRegistry.register(MetricRegistry.name("jvm", "thread-states"), new ThreadStatesGaugeSet());
        mMetricsRegistry.register(MetricRegistry.name("jvm", "fd", "usage"), new FileDescriptorRatioGauge());
    }

    private void initializeHealthChecks() {
        mHealthRegistry.register("database", new DatabaseHealthCheck(mDatabase));
    }

    public HealthCheckRegistry getHealthCheckRegistry() {
        return mHealthRegistry;
    }
}
