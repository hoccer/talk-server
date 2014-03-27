package com.hoccer.talk.server;

import better.jsonrpc.core.JsonRpcConnection;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.log4j.Logger;

import static com.codahale.metrics.MetricRegistry.name;

public class TalkMetricStats implements ITalkServerStatistics {

    private static final Logger LOG = Logger.getLogger(TalkMetricStats.class);
    MetricRegistry mMetricsRegistry;

    // Meters
    private final Meter clientRegistrationSucceededMeter;
    private final Meter clientRegistrationFailedMeter;
    private final Meter clientLoginsSRP1SucceededMeter;
    private final Meter clientLoginsSRP1FailedMeter;
    private final Meter clientLoginsSRP2SucceededMeter;
    private final Meter clientLoginsSRP2FailedMeter;
    private final Meter messageAcceptedSucceededMeter;
    private final Meter messageConfirmedSucceededMeter;
    private final Meter messageAcknowledgedSucceededMeter;

    //Timers
    private final Timer mRequestTimer;

    public TalkMetricStats(MetricRegistry metrics) {
        mMetricsRegistry = metrics;

        // Meters
        clientRegistrationSucceededMeter = metrics.meter(name(TalkServer.class, "client-registrations-succeeded-meter"));
        clientRegistrationFailedMeter = metrics.meter(name(TalkServer.class, "client-registrations-failed-meter"));
        clientLoginsSRP1SucceededMeter = metrics.meter(name(TalkServer.class, "client-logins-srp1-succeeded-meter"));
        clientLoginsSRP1FailedMeter = metrics.meter(name(TalkServer.class, "client-logins-srp1-failed-meter"));
        clientLoginsSRP2SucceededMeter = metrics.meter(name(TalkServer.class, "client-logins-srp2-succeeded-meter"));
        clientLoginsSRP2FailedMeter = metrics.meter(name(TalkServer.class, "client-logins-srp2-failed-meter"));
        messageAcceptedSucceededMeter = metrics.meter(name(TalkServer.class, "message-accepts-succeeded-meter"));
        messageConfirmedSucceededMeter = metrics.meter(name(TalkServer.class, "message-confirmations-succeeded-meter"));
        messageAcknowledgedSucceededMeter = metrics.meter(name(TalkServer.class, "message-acknowledgements-succeeded-meter"));

        // Timers
        mRequestTimer = mMetricsRegistry.timer("requests");
    }

    @Override
    public void signalClientRegisteredSucceeded() {
        clientRegistrationSucceededMeter.mark();
    }

    @Override
    public void signalClientRegisteredFailed() {
        clientRegistrationFailedMeter.mark();
    }

    @Override
    public void signalClientLoginSRP1Succeeded() {
        clientLoginsSRP1SucceededMeter.mark();
    }

    @Override
    public void signalClientLoginSRP1Failed() {
        clientLoginsSRP1FailedMeter.mark();
    }

    @Override
    public void signalClientLoginSRP2Succeeded() {
        clientLoginsSRP2SucceededMeter.mark();
    }

    @Override
    public void signalClientLoginSRP2Failed() {
        clientLoginsSRP2FailedMeter.mark();
    }

    @Override
    public void signalMessageAcceptedSucceeded() {
        messageAcceptedSucceededMeter.mark();
    }

    @Override
    public void signalMessageConfirmedSucceeded() {
        messageConfirmedSucceededMeter.mark();
    }

    @Override
    public void signalMessageAcknowledgedSucceeded() {
        messageAcknowledgedSucceededMeter.mark();
    }

    @Override
    public Timer.Context signalRequestStart(JsonRpcConnection connection, ObjectNode request) {
        return mRequestTimer.time();
    }

    @Override
    public void signalRequestStop(JsonRpcConnection connection, ObjectNode request, Timer.Context timerContext) {
        timerContext.stop();
    }

}

