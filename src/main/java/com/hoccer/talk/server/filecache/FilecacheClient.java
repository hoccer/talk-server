package com.hoccer.talk.server.filecache;

import better.jsonrpc.client.JsonRpcClient;
import better.jsonrpc.websocket.JsonRpcWsClient;
import com.hoccer.talk.filecache.rpc.ICacheControl;
import com.hoccer.talk.rpc.ITalkRpcServer;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wrapper for filecache control operations
 *
 * This automatically connects to the filecache when needed
 * and performs the requested operations synchronously.
 */
public class FilecacheClient extends JsonRpcWsClient {

    private static final Logger LOG = Logger.getLogger(FilecacheClient.class);

    TalkServer mServer;

    TalkServerConfiguration mConfig;

    JsonRpcClient mClient;

    ICacheControl mRpc;

    public FilecacheClient(TalkServer server) {
        super(server.getConfiguration().getFilecacheControlUrl(), "com.hoccer.talk.filecache.control.v1");
        mServer = server;
        mConfig = mServer.getConfiguration();
        mClient = new JsonRpcClient();
        mClient.setRequestTimeout(3000); // XXX constant
        this.bindClient(mClient);
        mRpc = this.makeProxy(ICacheControl.class);
    }

    private synchronized void ensureConnected() {
        if(!isConnected()) {
            LOG.info("filecache not connected, trying to connect");
            try {
                connect(5, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new RuntimeException("Timeout connecting to filecache", e);
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while connecting to filecache", e);
            } catch (IOException e) {
                throw new RuntimeException("Could not connect to filecache", e);
            }
        }
    }

    public ITalkRpcServer.FileHandles createFileForStorage(String accountId, String contentType, int contentLength) {
        ensureConnected();
        ICacheControl.FileHandles cacheHandles = mRpc.createFileForStorage(accountId, contentType, contentLength);
        LOG.info("created storage file with handles (fileId: '" + cacheHandles.fileId + "', "
                 + " uploadId: '" + cacheHandles.uploadId + "', "
                 + " downloadId: '" + cacheHandles.downloadId + "')");
        ITalkRpcServer.FileHandles serverHandles = new ITalkRpcServer.FileHandles();
        serverHandles.fileId = cacheHandles.fileId;
        serverHandles.uploadUrl = mConfig.getFilecacheUploadBase() + cacheHandles.uploadId;
        serverHandles.downloadUrl = mConfig.getFilecacheDownloadBase() + cacheHandles.downloadId;
        return serverHandles;
    }

    public ITalkRpcServer.FileHandles createFileForTransfer(String accountId, String contentType, int contentLength) {
        ensureConnected();
        ICacheControl.FileHandles cacheHandles = mRpc.createFileForTransfer(accountId, contentType, contentLength);
        LOG.info("created transfer file with handles (fileId: '" + cacheHandles.fileId + "', "
                 + " uploadId: '" + cacheHandles.uploadId + "', "
                 + " downloadId: '" + cacheHandles.downloadId + "')");
        ITalkRpcServer.FileHandles serverHandles = new ITalkRpcServer.FileHandles();
        serverHandles.fileId = cacheHandles.fileId;
        serverHandles.uploadUrl = mConfig.getFilecacheUploadBase() + cacheHandles.uploadId;
        serverHandles.downloadUrl = mConfig.getFilecacheDownloadBase() + cacheHandles.downloadId;
        return serverHandles;
    }

    public void deleteFile(String fileId) {
        ensureConnected();
        LOG.info("deleting file with id: '" + fileId + "'");
        mRpc.deleteFile(fileId);
    }

}
