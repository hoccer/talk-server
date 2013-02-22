package com.hoccer.talk.server;

import java.net.InetSocketAddress;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.websocket.WebSocketHandler;

public class Main {

	public static void main(String[] args) {
		Server s = new Server(new InetSocketAddress("localhost", 8080));
		try {
			TalkServer ts = new TalkServer();
			
			DefaultHandler clientDefHandler = new DefaultHandler();
			clientDefHandler.setServeIcon(false);
			
			WebSocketHandler clientHandler = new TalkRpcHandler(ts);
			clientHandler.setHandler(clientDefHandler);
			
			s.setHandler(clientHandler);
			
			s.start();
			s.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
