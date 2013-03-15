package com.hoccer.talk.server;

import java.util.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.hoccer.talk.rpc.ITalkRpcServer;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.push.PushAgent;
import com.hoccer.talk.server.push.PushRequest;
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
	private static final Logger log = Logger.getLogger(TalkServer.class);

    /** server-global JSON mapper */
	ObjectMapper mMapper;

    /** JSON-RPC server instance */
	JsonRpcServer mRpcServer;

    /** Push service agent */
	PushAgent mPushAgent;

    /** All connections (every connected websocket) */
	Vector<TalkRpcConnection> mConnections =
			new Vector<TalkRpcConnection>();

    /** All logged-in connections by client ID */
	Hashtable<String, TalkRpcConnection> mConnectionsByClientId =
			new Hashtable<String, TalkRpcConnection>();

    /**
     * Create and initialize a Hoccer Talk server
     */
	public TalkServer() {
		mMapper = createObjectMapper();
		mRpcServer = new JsonRpcServer(ITalkRpcServer.class);
		mPushAgent = new PushAgent();
	}
	
	public ObjectMapper getMapper() {
		return mMapper;
	}
	
	public JsonRpcServer getRpcServer() {
		return mRpcServer;
	}

    private ObjectMapper createObjectMapper() {
        ObjectMapper result = new ObjectMapper();
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return result;
    }

    /** XXX highly temporary */
    public List<String> getAllClients() {
        log.info("getAllClients()");
        Enumeration<String> k = mConnectionsByClientId.keys();
        List<String> r = new ArrayList<String>();
        while(k.hasMoreElements()) {
            r.add(k.nextElement());
        }
        return r;
    }
	
	public void identifyClient(TalkClient client, TalkRpcConnection connection) {
		mConnectionsByClientId.put(client.getClientId(), connection);
	}
	
	public void notifyClient(TalkClient client) {
		TalkRpcConnection connection =
				mConnectionsByClientId.get(client.getClientId());
		
		if(connection != null && connection.isConnected()) {
			return;
		}
		
		mPushAgent.submitRequest(new PushRequest(client));
	}
	
	public void connectionOpened(TalkRpcConnection connection) {
		log.info("connection opened");
		mConnections.add(connection);
	}

	public void connectionClosed(TalkRpcConnection connection) {
		log.info("connection closed");
		mConnections.remove(connection);
	}
	
}
