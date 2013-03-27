package com.hoccer.talk.server.rpc;

import javax.servlet.http.HttpServletRequest;

import better.jsonrpc.server.JsonRpcServer;
import java.util.logging.Logger;

import com.hoccer.talk.logging.HoccerLoggers;
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

	private static final Logger log = HoccerLoggers.getLogger(TalkRpcConnectionHandler.class);

    /** JSON object mapper common to all connections */
	ObjectMapper mMapper;

    /** Talk server instance */
	TalkServer mTalkServer;

    /** JSON-RPC server object */
    JsonRpcServer mRpcServer;

    /**
     * Construct a handler for the given server
     * @param server to add connections to
     */
	public TalkRpcConnectionHandler(TalkServer server) {
		mMapper = server.getMapper();
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
        // create JSON-RPC connection (this implements the websocket interface)
		JsonRpcWsConnection connection = new JsonRpcWsConnection(mMapper);
		connection.setServer(mRpcServer);
        // create talk high-level connection object
		TalkRpcConnection rpcConnection = new TalkRpcConnection(mTalkServer, connection);
        // return the raw connection (will be called by server for incoming messages)
		return connection;
	}

}
