package com.hoccer.talk.server.rpc;

import better.jsonrpc.core.JsonRpcConnection;
import better.jsonrpc.util.ProtocolUtils;
import better.jsonrpc.websocket.JsonRpcWsConnection;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.HashMap;

/**
 * Connection object representing one JSON-RPC connection each
 * <p/>
 * This class is responsible for the connection lifecycle as well
 * as for binding JSON-RPC handlers and Talk messaging state to
 * the connection based on user identity and state.
 */
public class TalkRpcConnection implements JsonRpcConnection.Listener, JsonRpcConnection.ConnectionEventListener {

    /**
     * Logger for connection-related things
     */
    private static final Logger LOG = Logger.getLogger(TalkRpcConnection.class);

    /**
     * Server this connection belongs to
     */
    private final TalkServer mServer;

    /**
     * JSON-RPC connection object
     */
    private final JsonRpcWsConnection mConnection;

    /**
     * HTTP request that created this WS connection
     */
    private final HttpServletRequest mInitialRequest;

    /**
     * RPC interface to client
     */
    private final ITalkRpcClient mClientRpc;

    /**
     * Last time we have seen client activity (connection or message)
     * *Note:* This does not seem to do anything!
     */
    private long mLastActivity;

    /**
     * Client object (if logged in)
     */
    private TalkClient mTalkClient;

    /**
     * Client id provided for client registration
     */
    private String mUnregisteredClientId;

    /**
     * Support mode flag
     */
    private boolean mSupportMode;

    /**
     * User data associated to requests
     */
    private final HashMap<Object, Timer.Context> requestTimers= new HashMap<Object, Timer.Context>();

    /**
     * Construct a connection for the given server using the given connection
     *
     * @param server     that we are part of
     * @param connection that we should handle
     */
    public TalkRpcConnection(TalkServer server, JsonRpcWsConnection connection, HttpServletRequest request) {
        // remember stuff
        mServer = server;
        mConnection = connection;
        mInitialRequest = request;
        // create a json-rpc proxy for client notifications
        mClientRpc = connection.makeProxy(ITalkRpcClient.class);
        // register ourselves for connection events
        mConnection.addListener(this);
        mConnection.addConnectionEventListener(this);
    }

    /**
     * Get the connection ID of this connection
     * <p/>
     * This is derived from JSON-RPC stuff at the moment.
     * <p/>
     * The only purpose of this is for identifying log messages.
     *
     * @return JsonRpcConnection connection
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
        return isConnected() && mTalkClient != null;
    }

    /**
     * Indicate if the client was logged in before the connection was closed
     */
    public boolean wasLoggedIn() {
        return mTalkClient != null;
    }

    /**
     * Returns the logged-in client or null
     *
     * @return TalkClient client
     */
    public TalkClient getClient() {
        return mTalkClient;
    }

    /**
     * Returns the logged-in clients id or null
     *
     * @return TalkClient client
     */
    public String getClientId() {
        if (mTalkClient != null) {
            return mTalkClient.getClientId();
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
     *  returns the connections server call handler
     */
    public ITalkRpcServer getServerHandler() {
        return (ITalkRpcServer)mConnection.getServerHandler();

    }
    /**
     * Returns the remote network address of the client
     */
    public String getRemoteAddress() {
        return mInitialRequest.getRemoteAddr();
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
     * @return true if in support mode
     */
    public boolean isSupportMode() {
        return mSupportMode;
    }

    /**
     * Callback: underlying connection is now open
     *
     * @param connection which was opened
     */
    @Override
    public void onOpen(JsonRpcConnection connection) {
        LOG.info("[connectionId: '" + getConnectionId() + "'] connection opened by " + getRemoteAddress());
        // reset the time of last activity
        mLastActivity = System.currentTimeMillis();
        // tell the server about the connection
        mServer.connectionOpened(this);
    }

    /**
     * Callback: underlying connection is now closed
     *
     * @param connection which was closed
     */
    @Override
    public void onClose(JsonRpcConnection connection) {
        LOG.info("[connectionId: '" + getConnectionId() + "'] connection closed");
        // invalidate the time of last activity
        mLastActivity = -1;
        // tell the server about the disconnect
        mServer.connectionClosed(this);
    }

    /**
     * Disconnect the underlying connection and finish up
     */
    public void disconnect() {
        mTalkClient = null;
        mConnection.disconnect();
    }

    /**
     * Called by handler when the client has logged in
     */
    public void identifyClient(String clientId) {
        LOG.info("[connectionId: '" + getConnectionId() + "'] logged in as " + clientId);

        ITalkServerDatabase database = mServer.getDatabase();

        // mark connection as logged in
        mTalkClient = database.findClientById(clientId);
        if (mTalkClient == null) {
            throw new RuntimeException("Client does not exist");
        } else {
            mServer.identifyClient(mTalkClient, this);
        }

        // update login time
        mTalkClient.setTimeLastLogin(new Date());
        database.saveClient(mTalkClient);

        // tell the client if it doesn't have push
        if (!mTalkClient.isPushCapable()) {
            mClientRpc.pushNotRegistered();
        }

        // attempt to deliver anything we might have
        mServer.getDeliveryAgent().requestDelivery(mTalkClient.getClientId());

        // request a ping in a few seconds
        mServer.getPingAgent().requestPing(mTalkClient.getClientId());
    }

    /**
     * Begins the registration process under the given client id
     */
    public void beginRegistration(String clientId) {
        LOG.info("[connectionId: '" + getConnectionId() + "'] begins registration as " + clientId);
        mUnregisteredClientId = clientId;
    }

    /**
     * Activate support mode for this connection
     */
    public void activateSupportMode() {
        LOG.info("[connectionId: '" + getConnectionId() + "'] activated support mode");
        mSupportMode = true;
    }

    /**
     * Deactivate support mode for this connection
     */
    public void deactivateSupportMode() {
        LOG.info("[connectionId: '" + getConnectionId() + "'] deactivated support mode");
        mSupportMode = false;
    }

    @Override
    public void onPreHandleRequest(JsonRpcConnection connection, ObjectNode request) {
        LOG.trace("onPreHandleRequest -- connectionId: '" +
                connection.getConnectionId() + "', clientId: '" +
                ((mTalkClient == null) ? "null" : mTalkClient.getClientId()) + "'");

        Timer.Context timerContext = mServer.getStatistics().signalRequestStart(connection, request);
        requestTimers.put(getIdFromRequest(request), timerContext);

        mServer.getUpdateAgent().setRequestContext();
        mServer.getDeliveryAgent().setRequestContext();
    }

    @Override
    public void onPostHandleRequest(JsonRpcConnection connection, ObjectNode request) {
        LOG.trace("onPostHandleRequest -- connectionId: '" +
                connection.getConnectionId() + "', clientId: '" +
                ((mTalkClient == null) ? "null" : mTalkClient.getClientId()) + "'");

        Object jsonRpcId = getIdFromRequest(request);
        mServer.getStatistics().signalRequestStop(connection, request, requestTimers.get(jsonRpcId));
        requestTimers.remove(jsonRpcId);

        mServer.getUpdateAgent().clearRequestContext();
        mServer.getDeliveryAgent().clearRequestContext();
    }

    @Override
    public void onPreHandleNotification(JsonRpcConnection connection, ObjectNode notification) {

    }

    @Override
    public void onPostHandleNotification(JsonRpcConnection connection, ObjectNode notification) {

    }

    @Override
    public void onPreHandleResponse(JsonRpcConnection connection, ObjectNode response) {

    }

    @Override
    public void onPostHandleResponse(JsonRpcConnection connection, ObjectNode response) {

    }

    private static Object getIdFromRequest(ObjectNode request) {
        return ProtocolUtils.parseId(request.get("id"));
    }
}
