package com.hoccer.talk.server;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.delivery.DeliveryAgent;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.group.GroupAgent;
import com.hoccer.talk.server.ping.PingAgent;
import com.hoccer.talk.server.update.UpdateAgent;
import com.hoccer.talk.server.push.PushAgent;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import better.jsonrpc.server.JsonRpcServer;

/**
 * Main object of the Talk server
 *
 * This holds global state such as the list of active connections,
 * references to common database mapping helpers and so on.
 */
public class TalkServer {

    /** Logger for changes in global server state */
	private static final Logger log = HoccerLoggers.getLogger(TalkServer.class);

    /** server-global JSON mapper */
	ObjectMapper mMapper;

    /** JSON-RPC server instance */
	JsonRpcServer mRpcServer;

    /** Server configuration */
    TalkServerConfiguration mConfiguration;

    /** Database accessor */
    ITalkServerDatabase mDatabase;

    /** Delivery agent */
    DeliveryAgent mDeliveryAgent;

    /** Group update agent */
    GroupAgent mGroupAgent;

    /** Push service agent */
	PushAgent mPushAgent;

    /** Presence update agent */
    UpdateAgent mUpdateAgent;

    /** Ping measurement agent */
    PingAgent mPingAgent;

    /** All connections (every connected websocket) */
	Vector<TalkRpcConnection> mConnections =
			new Vector<TalkRpcConnection>();

    /** All logged-in connections by client ID */
	Hashtable<String, TalkRpcConnection> mConnectionsByClientId =
			new Hashtable<String, TalkRpcConnection>();

    /**
     * Create and initialize a Hoccer Talk server
     */
	public TalkServer(TalkServerConfiguration configuration, ITalkServerDatabase database) {
        mConfiguration = configuration;
        mDatabase = database;
		mMapper = createObjectMapper();
		mRpcServer = new JsonRpcServer(ITalkRpcServer.class);
        mDeliveryAgent = new DeliveryAgent(this);
        mGroupAgent = new GroupAgent(this);
		mPushAgent = new PushAgent(this);
        mUpdateAgent = new UpdateAgent(this);
        mPingAgent = new PingAgent(this);
    }

    /** @return the object mapper used by this server */
	public ObjectMapper getMapper() {
		return mMapper;
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

    /** @return  the group agent of this server */
    public GroupAgent getGroupAgent() {
        return mGroupAgent;
    }

    /** @return the update agent of this server */
    public UpdateAgent getUpdateAgent() {
        return mUpdateAgent;
    }

    /** @return the ping agent of this server */
    public PingAgent getPingAgent() {
        return mPingAgent;
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
		mConnections.add(connection);
	}

    /**
     * Unregister a connection from the server
     * @param connection to be removed
     */
	public void connectionClosed(TalkRpcConnection connection) {
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
	
}
