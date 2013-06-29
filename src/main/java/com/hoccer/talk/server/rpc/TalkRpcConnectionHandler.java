package com.hoccer.talk.server.rpc;

import javax.servlet.http.HttpServletRequest;

import better.jsonrpc.client.JsonRpcClient;
import better.jsonrpc.server.JsonRpcServer;

import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

import better.jsonrpc.websocket.JsonRpcWsConnection;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hoccer.talk.server.TalkServer;

/**
 * WebSocket handler
 *
 * This is responsible for accepting websocket connections,
 * creating and configuring a connection object for them.
 *
 */
public class TalkRpcConnectionHandler extends WebSocketHandler {

	private static final Logger log = Logger.getLogger(TalkRpcConnectionHandler.class);

    /** Talk server instance */
	TalkServer mTalkServer;

    /** JSON-RPC server object */
    JsonRpcServer mRpcServer;

    /**
     * Construct a handler for the given server
     * @param server to add connections to
     */
	public TalkRpcConnectionHandler(TalkServer server) {
        mRpcServer = server.getRpcServer();
		mTalkServer = server;
	}

    /**
     * Create a websocket for the given HTTP request and WS protocol
     * @param request
     * @param protocol
     * @return
     */
	@Override
	public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
        log.info("connection with protocol " + protocol);
        if(protocol.equals("com.hoccer.talk.v1")) {
            // create JSON-RPC connection (this implements the websocket interface)
            JsonRpcWsConnection connection = new JsonRpcWsConnection(mTalkServer.getJsonMapper());
            // create talk high-level connection object
            TalkRpcConnection rpcConnection = new TalkRpcConnection(mTalkServer, connection, request);
            // configure the connection
            connection.setMaxIdleTime(1800 * 1000);
            connection.bindClient(new JsonRpcClient());
            connection.bindServer(mRpcServer, new TalkRpcHandler(mTalkServer, rpcConnection));
            // return the raw connection (will be called by server for incoming messages)
            return connection;
        }
        if(protocol.equals("com.hoccer.talk.v1.bson")) {
            // create JSON-RPC connection (this implements the websocket interface)
            JsonRpcWsConnection connection = new JsonRpcWsConnection(mTalkServer.getBsonMapper());
            // create talk high-level connection object
            TalkRpcConnection rpcConnection = new TalkRpcConnection(mTalkServer, connection, request);
            // configure the connection
            connection.setSendBinaryMessages(true);
            connection.setMaxIdleTime(1800 * 1000);
            connection.bindClient(new JsonRpcClient());
            connection.bindServer(mRpcServer, new TalkRpcHandler(mTalkServer, rpcConnection));
            // return the raw connection (will be called by server for incoming messages)
            return connection;
        }

        log.info("new connection with unknown protocol " + protocol);
        return null;
	}

}
