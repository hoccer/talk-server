package com.hoccer.talk.server;

import java.util.UUID;

import org.apache.log4j.Logger;

import better.jsonrpc.core.JsonRpcConnection;
import better.jsonrpc.util.ProxyUtil;

import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.TalkRpcClient;
import com.hoccer.talk.rpc.TalkRpcServer;

public class TalkRpcConnection implements TalkRpcServer, JsonRpcConnection.Listener {
	
	private static final Logger log = Logger.getLogger(TalkRpcConnection.class);
	
	/** Server this connection belongs to */
	TalkServer mServer;
	
	/** JSON-RPC connection object */
	JsonRpcConnection mConnection;
	
	/** RPC interface to client */
	TalkRpcClient mClient;
	
	public TalkRpcConnection(TalkServer server, JsonRpcConnection connection) {
		mServer = server;
		mConnection = connection;
		mClient = ProxyUtil.createClientProxy(
				this.getClass().getClassLoader(),
				TalkRpcClient.class,
				mConnection);
	}

	@Override
	public void onOpen(JsonRpcConnection connection) {
		mServer.connectionOpened(this);
	}

	@Override
	public void onClose(JsonRpcConnection connection) {
		mServer.connectionClosed(this);
	}

	@Override
	public void identify(String clientId) {
		log.info("client identifies as " + clientId);
	}

	@Override
	public void requestOutgoingDelivery(boolean wanted) {
	}

	@Override
	public void requestIncomingDelivery(boolean wanted) {
	}

	@Override
	public void deliveryRequest(TalkMessage m, TalkDelivery[] d) {
		m.setMessageId(UUID.randomUUID().toString());
		log.info("requesting delivery of msg " + m.getMessageId()
					+ " to " + d.length + " clients");
	}

	@Override
	public void deliveryConfirm(TalkDelivery d) {
		log.info("delivery confirmed");
		mClient.outgoingDelivery(d);
	}

}
