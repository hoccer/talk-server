package com.hoccer.talk.server;

import java.util.UUID;

import org.apache.log4j.Logger;

import better.jsonrpc.core.JsonRpcConnection;
import better.jsonrpc.util.ProxyUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.rpc.TalkRpcClient;
import com.hoccer.talk.rpc.TalkRpcServer;

public class TalkRpcConnection implements JsonRpcConnection.Listener {
	
	private static final Logger log = Logger.getLogger(TalkRpcConnection.class);
	
	/** Server this connection belongs to */
	TalkServer mServer;
	
	/** JSON-RPC connection object */
	JsonRpcConnection mConnection;
	
	/** JSON-RPC handler object */
	TalkRpcServerImpl mHandler;
	
	/** RPC interface to client */
	TalkRpcClient mClientRpc;
	
	/** Client object, if logged in */
	TalkClient mClient;
	
	/** Last time we have seen client activity (connection or message) */
	long mLastActivity;
	
	public TalkRpcConnection(TalkServer server, JsonRpcConnection connection) {
		mServer = server;
		mConnection = connection;
		mHandler = new TalkRpcServerImpl();
		mClientRpc = ProxyUtil.createClientProxy(
				this.getClass().getClassLoader(),
				TalkRpcClient.class,
				mConnection);
		mConnection.setHandler(mHandler);
		mConnection.addListener(this);
	}
	
	public boolean isConnected() {
		return mConnection != null && mConnection.isConnected();
	}

	@Override
	public void onOpen(JsonRpcConnection connection) {
		mLastActivity = System.currentTimeMillis();
		mServer.connectionOpened(this);
	}

	@Override
	public void onClose(JsonRpcConnection connection) {
		mLastActivity = -1;
		mServer.connectionClosed(this);
	}
	
	@Override
	public void onMessageSent(JsonRpcConnection connection, ObjectNode message) {
	}

	@Override
	public void onMessageReceived(JsonRpcConnection connection, ObjectNode message) {
		mLastActivity = System.currentTimeMillis();
	}

	public class TalkRpcServerImpl implements TalkRpcServer {
	
		@Override
		public void identify(String clientId) {
			log.info("client identifies as " + clientId);
			mClient = TalkDatabase.findClient(clientId);
			mServer.identifyClient(mClient, TalkRpcConnection.this);
		}
	
		@Override
		public void requestOutgoingDelivery(boolean wanted) {
		}
	
		@Override
		public void requestIncomingDelivery(boolean wanted) {
		}
	
		@Override
		public void deliveryRequest(TalkMessage m, TalkDelivery[] d) {
		}
	
		@Override
		public void deliveryConfirm(String messageId) {
		}
	
	}

}
