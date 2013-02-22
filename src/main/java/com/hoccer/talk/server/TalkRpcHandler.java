package com.hoccer.talk.server;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

import better.jsonrpc.websocket.JsonRpcWsConnection;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TalkRpcHandler extends WebSocketHandler {

	private static final Logger log = Logger.getLogger(TalkRpcHandler.class);

	ObjectMapper mMapper;

	TalkServer mServer;

	public TalkRpcHandler(TalkServer server) {
		mMapper = server.getMapper();
		mServer = server;
	}

	@Override
	public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
		JsonRpcWsConnection connection = new JsonRpcWsConnection(mMapper);
		connection.setServer(mServer.getRpcServer());
		TalkRpcConnection rpcConnection = new TalkRpcConnection(mServer, connection);
		connection.setHandler(rpcConnection);
		connection.addListener(rpcConnection);
		return connection;
	}

}
