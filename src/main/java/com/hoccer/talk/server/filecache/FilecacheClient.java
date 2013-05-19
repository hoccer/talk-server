package com.hoccer.talk.server.filecache;

import better.jsonrpc.client.JsonRpcClient;
import better.jsonrpc.websocket.JsonRpcWsClient;
import com.hoccer.talk.filecache.rpc.ICacheControl;
import com.hoccer.talk.server.TalkServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FilecacheClient extends JsonRpcWsClient {

    private static final Logger LOG = Logger.getLogger(FilecacheClient.class);

    TalkServer mServer;

    JsonRpcClient mClient;

    ICacheControl mRpc;

    public FilecacheClient(TalkServer server) {
        super(server.getConfiguration().getFilecacheControlUrl(), "com.hoccer.talk.filecache.control.v1");
        mServer = server;
        mClient = new JsonRpcClient();
        this.bindClient(mClient);
        mRpc = (ICacheControl)this.makeProxy(ICacheControl.class);
    }

    private void ensureConnected() {
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

    public void createFileForStorage() {
        ensureConnected();
        ICacheControl.FileHandles handles = mRpc.createFileForStorage("xxx", 2342);
        LOG.info("created file with handles f=" + handles.fileId + " u=" + handles.uploadId + " d=" + handles.downloadId);
    }

}
