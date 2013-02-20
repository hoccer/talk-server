package com.hoccer.talk.server;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.jetty.websocket.WebSocket;

import com.hoccer.talk.server.model.TalkDelivery;
import com.hoccer.talk.server.model.TalkMessage;

public class ClientSocket implements WebSocket.OnTextMessage {

	private static final Logger log = Logger.getLogger(ClientSocket.class);
	
	private Connection connection;
	
		
	public ClientSocket() {
	}
	
	@Override
	public void onOpen(Connection connection) {
		log.info("connection open");
		this.connection = connection;
	}

	@Override
	public void onClose(int closeCode, String message) {
		log.info("connection close");
	}

	@Override
	public void onMessage(String data) {
		
	}
	
	interface C2S {
		void identify(String clientId);
		
		void requestPresence(boolean wanted);
		void requestDeliveryOut(boolean wanted);
		void requestDeliveryIn(boolean wanted);
		
		void deliveryRequest(TalkMessage m, TalkDelivery[] d);
		void deliveryConfirm(TalkDelivery d);
		
	}
	
	interface S2C {
		void presence();
		void deliveryIn(TalkDelivery d, TalkMessage m);
		void deliveryOut(TalkDelivery d);
	}

}
