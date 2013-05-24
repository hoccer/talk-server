package com.hoccer.talk.server.ping;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PingAgent {

    private static final Logger LOG = Logger.getLogger(PingAgent.class);

    TalkServer mServer;

    ScheduledExecutorService mExecutor;

    AtomicInteger mPingRequests = new AtomicInteger();
    AtomicInteger mPingAttempts = new AtomicInteger();

    AtomicInteger mPingFailures = new AtomicInteger();
    AtomicInteger mPingSuccesses = new AtomicInteger();

    Timer mPingLatency;

    public PingAgent(TalkServer server) {
        mServer = server;
        mExecutor = Executors.newScheduledThreadPool(TalkServerConfiguration.THREADS_PING);
        initializeMetrics(mServer.getMetrics());
    }

    private void initializeMetrics(MetricRegistry metrics) {
        metrics.register(MetricRegistry.name(PingAgent.class, "requests"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingRequests.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "attempts"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingAttempts.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "failures"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingFailures.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "successes"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingSuccesses.intValue();
                    }
                });
        mPingLatency = metrics.timer(MetricRegistry.name(PingAgent.class, "latency"));
    }

    public void requestPing(final String clientId) {
        mPingRequests.incrementAndGet();
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                TalkRpcConnection conn = mServer.getClientConnection(clientId);
                if(conn != null) {
                    ITalkRpcClient rpc = conn.getClientRpc();
                    mPingAttempts.incrementAndGet();
                    Timer.Context timer = mPingLatency.time();
                    try {
                        rpc.ping();
                        mPingSuccesses.incrementAndGet();
                    } catch (Throwable t) {
                        LOG.info("exception in ping on " + clientId, t);
                        mPingFailures.incrementAndGet();
                    } finally {
                        timer.stop();
                    }
                }
            }
        }, 3, TimeUnit.SECONDS);
    }

}
