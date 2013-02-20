package com.hoccer.talk.server;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketHandler;

public class ClientSocketHandler extends WebSocketHandler {

	@Override
	public WebSocket doWebSocketConnect(HttpServletRequest request,
			String protocol) {
		//if(protocol.equals("com.hoccer.talk.v1")) {
			return new ClientSocket();
		//}
		//return null;
	}

}
