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

/**
 * Connection object representing one JSON-RPC connection each
 *
 * This class is responsible for the connection lifecycle as well
 * as for binding JSON-RPC handlers and Talk messaging state to
 * the connection based on user identity and state.
 *
 */
public class TalkRpcConnection implements JsonRpcConnection.Listener {

    /** Logger for connection-related things */
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

    /**
     * Construct a connection for the given server using the given connection
     *
     * @param server that we are part of
     * @param connection that we should handle
     */
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

    /**
     * Indicate if the connection is currently connected
     *
     * @return true if connected
     */
	public boolean isConnected() {
		return mConnection != null && mConnection.isConnected();
	}

    /**
     * Callback: underlying connection is now open
     * @param connection
     */
	@Override
	public void onOpen(JsonRpcConnection connection) {
        // reset the time of last activity
		mLastActivity = System.currentTimeMillis();
        // tell the server about the connection
		mServer.connectionOpened(this);
	}

    /**
     * Callback: underlying connection is now closed
     * @param connection
     */
	@Override
	public void onClose(JsonRpcConnection connection) {
        // invalidate the time of last activity
		mLastActivity = -1;
        // tell the server about the disconnect
		mServer.connectionClosed(this);
	}

    /**
     * RPC protocol implementation
     */
	public class TalkRpcServerImpl implements TalkRpcServer {
	
		@Override
		public void identify(String clientId) {
			log.info("client identifies as " + clientId);
			mClient = TalkDatabase.findClient(clientId);
			mServer.identifyClient(mClient, TalkRpcConnection.this);
		}
	
		@Override
		public TalkDelivery[] deliveryRequest(TalkMessage message, TalkDelivery[] deliveries) {
            log.info("client requests delivery of new message to "
                    + deliveries.length + " clients");
            for(TalkDelivery d: deliveries) {
                String receiverId = d.getReceiverId();
                if(receiverId.equals(mClient.getClientId())) {
                    log.info("delivery rejected: send to self");
                    // mark delivery failed
                } else {
                    TalkClient receiver = TalkDatabase.findClient(receiverId);
                    if(receiver == null) {
                        log.info("delivery rejected: client " + receiverId + " does not exist");
                        // mark delivery failed
                    } else {
                        log.info("delivery accepted: client " + receiverId);
                        // delivery accepted, save
                        TalkDatabase.saveDelivery(d);
                    }
                }
            }
            return deliveries;
		}
	
		@Override
		public TalkDelivery deliveryConfirm(String messageId) {
            log.info("client confirms delivery of message " + messageId);
            TalkDelivery d = TalkDatabase.findDelivery(messageId, mClient.getClientId());
            if(d == null) {
                log.info("confirmation ignored: no delivery of message "
                        + messageId + " for client " + mClient.getClientId());
            } else {
                log.info("confirmation accepted: message "
                        + messageId + " for client " + mClient.getClientId());
            }
            return d;
		}
	
	}

}
