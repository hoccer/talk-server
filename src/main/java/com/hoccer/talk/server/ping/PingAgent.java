package com.hoccer.talk.server.ping;

import better.jsonrpc.client.JsonRpcClientDisconnect;
import better.jsonrpc.client.JsonRpcClientTimeout;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import com.hoccer.talk.util.NamedThreadFactory;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ping measurement agent
 * <p/>
 * This gets kicked on login and reports the round-trip call latency
 * from the server to the client and back.
 * <p/>
 * It is intended purely for monitoring.
 */
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
        mExecutor = Executors.newScheduledThreadPool(
            TalkServerConfiguration.THREADS_PING,
            new NamedThreadFactory("ping-agent")
        );
        initializeMetrics(mServer.getMetrics());

        if (TalkServerConfiguration.PERFORM_PING_AT_INTERVALS) {
            schedulePingAllReadyClients();
        } else {
            LOG.info("Not scheduling regular ping since it is deactivated by configuration.");
        }
    }

    private void schedulePingAllReadyClients () {
        LOG.info("Scheduling pinging of all ready clients to occur in '" + TalkServerConfiguration.PING_INTERVAL + "' seconds.");
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                pingReadyClients();
                schedulePingAllReadyClients();
            }
        }, TalkServerConfiguration.PING_INTERVAL, TimeUnit.SECONDS);
    }

    private void initializeMetrics(MetricRegistry metrics) {
        metrics.register(MetricRegistry.name(PingAgent.class, "pingRequests"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingRequests.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "pingAttempts"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingAttempts.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "pingFailures"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mPingFailures.intValue();
                    }
                });
        metrics.register(MetricRegistry.name(PingAgent.class, "pingSuccesses"),
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
                performPing(clientId);
            }
        }, 3, TimeUnit.SECONDS);
    }

    private void performPing(String clientId) {
        TalkRpcConnection conn = mServer.getClientConnection(clientId);
        if (conn != null) {
            ITalkRpcClient rpc = conn.getClientRpc();
            mPingAttempts.incrementAndGet();
            Timer.Context timer = mPingLatency.time();
            try {
                rpc.ping();
                long elapsed = (timer.stop() / 1000000);
                conn.setLastPingOccured(new Date());
                conn.setLastPingLatency(elapsed);
                LOG.debug("ping on " + clientId + " took " + elapsed + " msecs");
                mPingSuccesses.incrementAndGet();
            } catch (JsonRpcClientDisconnect e) {
                LOG.debug("ping on " + clientId + " disconnect");
                mPingFailures.incrementAndGet();
            } catch (JsonRpcClientTimeout e) {
                LOG.debug("ping on " + clientId + " timeout");
                mPingFailures.incrementAndGet();
            } catch (Throwable t) {
                LOG.error("exception in ping on " + clientId, t);
                mPingFailures.incrementAndGet();
            }
        }
    }

    private void pingReadyClients() {
        for (TalkRpcConnection connection : mServer.getReadyConnections()) {
            // LOG.info("pinging ready client: " + connection.getConnectionId() + " (clientId: " + connection.getClientId() + ")");
            performPing(connection.getClientId());
        }
    }

}
