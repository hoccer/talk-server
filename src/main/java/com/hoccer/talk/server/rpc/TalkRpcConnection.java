package com.hoccer.talk.server.rpc;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.rpc.ITalkRpcClient;

import java.util.logging.Logger;

import better.jsonrpc.core.JsonRpcConnection;

import com.hoccer.talk.server.TalkServer;

import javax.servlet.http.HttpServletRequest;

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
	private static final Logger LOG = HoccerLoggers.getLogger(TalkRpcConnection.class);
	
	/** Server this connection belongs to */
	TalkServer mServer;

	/** JSON-RPC connection object */
	JsonRpcConnection mConnection;

    /** HTTP request that created this WS connection */
    HttpServletRequest mInitialRequest;
	
	/** RPC interface to client */
	ITalkRpcClient mClientRpc;
	
	/** Last time we have seen client activity (connection or message) */
	long mLastActivity;

    /** Client object (if logged in) */
    TalkClient mClient;

    /** Client id provided for client registration */
    String mUnregisteredClientId;

    /**
     * Construct a connection for the given server using the given connection
     *
     * @param server that we are part of
     * @param connection that we should handle
     */
	public TalkRpcConnection(TalkServer server, JsonRpcConnection connection, HttpServletRequest request) {
        // remember stuff
		mServer = server;
		mConnection = connection;
        mInitialRequest = request;
        // create a json-rpc proxy for client notifications
		mClientRpc = (ITalkRpcClient)connection.makeProxy(ITalkRpcClient.class);
        // register ourselves for connection events
		mConnection.addListener(this);
	}

    /**
     * Get the connection ID of this connection
     *
     * This is derived from JSON-RPC stuff at the moment.
     *
     * The only purpose of this is for identifying log messages.
     *
     * @return
     */
    public int getConnectionId() {
        return mConnection.getConnectionId();
    }

    /**
     * Indicate if the connection is currently connected
     *
     * @return true if connected
     */
	public boolean isConnected() {
		return mConnection.isConnected();
	}

    /**
     * Indicate if the connection is currently logged in
     */
    public boolean isLoggedIn() {
        return isConnected() && mClient != null;
    }

    /**
     * Returns the logged-in client or null
     * @return
     */
    public TalkClient getClient() {
        return mClient;
    }

    /**
     * Returns the logged-in clients id or null
     * @return
     */
    public String getClientId() {
        if(mClient != null) {
            return mClient.getClientId();
        }
        return null;
    }

    /**
     * Returns the RPC interface to the client
     */
    public ITalkRpcClient getClientRpc() {
        return mClientRpc;
    }

    /**
     * Returns true if the client is engaging in registration
     */
    public boolean isRegistering() {
        return mUnregisteredClientId != null;
    }

    /**
     * Returns the client id this connection is currently registering
     */
    public String getUnregisteredClientId() {
        return mUnregisteredClientId;
    }

    /**
     * Callback: underlying connection is now open
     * @param connection
     */
	@Override
	public void onOpen(JsonRpcConnection connection) {
        LOG.info("[" + getConnectionId() + "] connection opened");
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
        LOG.info("[" + getConnectionId() + "] connection closed");
        // update presences
        if(isLoggedIn()) {
            mServer.getUpdateAgent().requestPresenceUpdate(getClientId());
        }
        // invalidate the time of last activity
		mLastActivity = -1;
        // tell the server about the disconnect
		mServer.connectionClosed(this);
	}

    /**
     * Called by handler when the client has logged in
     */
    public void identifyClient(String clientId) {
        LOG.info("[" + getConnectionId() + "] logged in as " + clientId);

        // mark connection as logged in
        mClient = mServer.getDatabase().findClientById(clientId);
        if(mClient == null) {
            throw new RuntimeException("Client does not exist");
        } else {
            mServer.identifyClient(mClient, this);
        }

        // tell the client if it doesn't have push
        if(!mClient.isPushCapable()) {
            mClientRpc.pushNotRegistered();
        }

        // attempt to deliver anything we might have
        mServer.getDeliveryAgent().requestDelivery(mClient.getClientId());
    }

    /**
     * Begins the registration process under the given client id
     */
    public void beginRegistration(String clientId) {
        LOG.info("[" + getConnectionId() + "] begins registration as " + clientId);
        mUnregisteredClientId = clientId;
    }

}
