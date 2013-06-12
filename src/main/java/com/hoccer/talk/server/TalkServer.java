package com.hoccer.talk.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.delivery.DeliveryAgent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.filecache.FilecacheClient;
import com.hoccer.talk.server.ping.PingAgent;
import com.hoccer.talk.server.update.UpdateAgent;
import com.hoccer.talk.server.push.PushAgent;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import better.jsonrpc.server.JsonRpcServer;
import org.apache.log4j.Logger;

/**
 * Main object of the Talk server
 *
 * This holds global state such as the list of active connections,
 * references to common database mapping helpers and so on.
 */
public class TalkServer {

    /** Logger for changes in global server state */
	private static final Logger log = Logger.getLogger(TalkServer.class);

    /** server-global JSON mapper */
	ObjectMapper mMapper;

    /** Metrics registry */
    MetricRegistry mMetrics;
    JmxReporter mJmxReporter;

    /** JSON-RPC server instance */
	JsonRpcServer mRpcServer;

    /** Server configuration */
    TalkServerConfiguration mConfiguration;

    /** Database accessor */
    ITalkServerDatabase mDatabase;

    /** Delivery agent */
    DeliveryAgent mDeliveryAgent;

    /** Push service agent */
	PushAgent mPushAgent;

    /** Presence update agent */
    UpdateAgent mUpdateAgent;

    /** Ping measurement agent */
    PingAgent mPingAgent;

    /** Client for the filecache control interface */
    FilecacheClient mFilecacheClient;

    /** All connections (every connected websocket) */
	Vector<TalkRpcConnection> mConnections =
			new Vector<TalkRpcConnection>();

    /** All logged-in connections by client ID */
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

		mMapper = createObjectMapper();

        mMetrics = new MetricRegistry();
        initializeMetrics();

		mRpcServer = new JsonRpcServer(ITalkRpcServer.class);
        mDeliveryAgent = new DeliveryAgent(this);
		mPushAgent = new PushAgent(this);
        mUpdateAgent = new UpdateAgent(this);
        mPingAgent = new PingAgent(this);
        mFilecacheClient = new FilecacheClient(this);

        mJmxReporter = JmxReporter.forRegistry(mMetrics).build();
        mJmxReporter.start();
    }

    /** @return the object mapper used by this server */
	public ObjectMapper getMapper() {
		return mMapper;
	}

    /** @return the metrics registry for the server */
    public MetricRegistry getMetrics() {
        return mMetrics;
    }

    /** @return the JSON-RPC server */
	public JsonRpcServer getRpcServer() {
		return mRpcServer;
	}

    /** @return the configuration of this server */
    public TalkServerConfiguration getConfiguration() {
        return mConfiguration;
    }

    /** @return the database accessor of this server */
    public ITalkServerDatabase getDatabase() {
        return mDatabase;
    }

    /** @return the push agent of this server */
    public PushAgent getPushAgent() {
        return mPushAgent;
    }

    /** @return the delivery agent of this server */
    public DeliveryAgent getDeliveryAgent() {
        return mDeliveryAgent;
    }

    /** @return the update agent of this server */
    public UpdateAgent getUpdateAgent() {
        return mUpdateAgent;
    }

    /** @return the ping agent of this server */
    public PingAgent getPingAgent() {
        return mPingAgent;
    }

    /** @return the filecache control client */
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
     * @param client that was logged in
     * @param connection the client is on
     */
	public void identifyClient(TalkClient client, TalkRpcConnection connection) {
        String clientId = client.getClientId();
		mConnectionsByClientId.put(clientId, connection);
        mUpdateAgent.requestPresenceUpdate(clientId);
	}

    /**
     * Register a new connection with the server
     * @param connection to be registered
     */
	public void connectionOpened(TalkRpcConnection connection) {
        mConnectionsTotal.incrementAndGet();
        mConnectionsOpen.incrementAndGet();
        mConnections.add(connection);
	}

    /**
     * Unregister a connection from the server
     * @param connection to be removed
     */
	public void connectionClosed(TalkRpcConnection connection) {
        mConnectionsOpen.decrementAndGet();
        // remove connection from list
		mConnections.remove(connection);
        // remove connection from table
        if(connection.getClientId() != null) {
            String clientId = connection.getClientId();
            mConnectionsByClientId.remove(clientId);
            mUpdateAgent.requestPresenceUpdate(clientId);
        }
        // disconnect if we still are
        if(connection.isConnected()) {
            connection.disconnect();
        }
	}

    /** Creates the object mapper for this server */
    private ObjectMapper createObjectMapper() {
        ObjectMapper result = new ObjectMapper();
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return result;
    }

    /** Set up server metrics */
    private void initializeMetrics() {
        mMetrics.register(MetricRegistry.name(TalkServer.class, "connectionsOpen"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mConnectionsOpen.intValue();
                    }
                });
        mMetrics.register(MetricRegistry.name(TalkServer.class, "connectionsTotal"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mConnectionsTotal.intValue();
                    }
                });
    }
	
}
