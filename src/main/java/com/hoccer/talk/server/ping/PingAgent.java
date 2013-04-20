package com.hoccer.talk.server.ping;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PingAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(PingAgent.class);

    TalkServer mServer;

    ScheduledExecutorService mExecutor;

    public PingAgent(TalkServer server) {
        mServer = server;
        mExecutor = Executors.newScheduledThreadPool(8);
    }

    public void requestPing(final String clientId) {
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                TalkRpcConnection conn = mServer.getClientConnection(clientId);
                if(conn != null) {
                    long start, end;
                    ITalkRpcClient rpc = conn.getClientRpc();
                    try {
                        start = System.currentTimeMillis();
                        rpc.ping();
                        end = System.currentTimeMillis();
                        long duration = end - start;
                        LOG.info("ping on " + clientId + " at " + conn.getRemoteAddress()
                                    + " took " + duration + " msecs");
                    } catch (Throwable t) {
                        LOG.log(Level.INFO, "exception in ping on " + clientId, t);
                    }
                }
            }
        }, 3, TimeUnit.SECONDS);
    }

}
