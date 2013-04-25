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

    /** All connections (every connected websocket) */
	Vector<TalkRpcConnection> mConnections =
			new Vector<TalkRpcConnection>();

    /** All logged-in connections by client ID */
	Hashtable<String, TalkRpcConnection> mConnectionsByClientId =
			new Hashtable<String, TalkRpcConnection>();

    /**
     * Create and initialize a Hoccer Talk server
     */
	public TalkServer(ITalkServerDatabase database) {
        mDatabase = database;
		mMapper = createObjectMapper();
		mRpcServer = new JsonRpcServer(ITalkRpcServer.class);
        mDeliveryAgent = new DeliveryAgent(this);
		mPushAgent = new PushAgent(this);
        mUpdateAgent = new UpdateAgent(this);
        mPingAgent = new PingAgent(this);
    }
	
	public ObjectMapper getMapper() {
		return mMapper;
	}
	
	public JsonRpcServer getRpcServer() {
		return mRpcServer;
	}

    public ITalkServerDatabase getDatabase() {
        return mDatabase;
    }

    public PushAgent getPushAgent() {
        return mPushAgent;
    }

    public DeliveryAgent getDeliveryAgent() {
        return mDeliveryAgent;
    }

    public UpdateAgent getUpdateAgent() {
        return mUpdateAgent;
    }

    public PingAgent getPingAgent() {
        return mPingAgent;
    }

    public boolean isClientConnected(String clientId) {
        return getClientConnection(clientId) != null;
    }

    public TalkRpcConnection getClientConnection(String clientId) {
        return mConnectionsByClientId.get(clientId);
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper result = new ObjectMapper();
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return result;
    }

    /** XXX highly temporary */
    public List<String> getAllClients() {
        Enumeration<String> k = mConnectionsByClientId.keys();
        List<String> r = new ArrayList<String>();
        while(k.hasMoreElements()) {
            r.add(k.nextElement());
        }
        return r;
    }
	
	public void identifyClient(TalkClient client, TalkRpcConnection connection) {
        String clientId = client.getClientId();
		mConnectionsByClientId.put(clientId, connection);
        mUpdateAgent.requestPresenceUpdate(clientId);
	}
	
	public void connectionOpened(TalkRpcConnection connection) {
		mConnections.add(connection);
	}

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
	
}
