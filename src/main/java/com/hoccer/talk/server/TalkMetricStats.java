package com.hoccer.talk.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

public class TalkMetricStats implements ITalkServerStatistics {

    Date startTime = new Date();

    MetricRegistry mMetrics;
    private final Meter clientRegistrationSucceededMeter;
    private final Meter clientRegistrationFailedMeter;
    private final Meter clientLoginsSRP1SucceededMeter;
    private final Meter clientLoginsSRP1FailedMeter;
    private final Meter clientLoginsSRP2SucceededMeter;
    private final Meter clientLoginsSRP2FailedMeter;
    private final Meter messageAcceptedSucceededMeter;
    private final Meter messageConfirmedSucceededMeter;
    private final Meter messageAcknowledgedSucceededMeter;


    public TalkMetricStats(MetricRegistry metrics) {
        mMetrics = metrics;

        // Meters
        clientRegistrationSucceededMeter = metrics.meter(name(TalkServer.class, "client-registrations-succeeded-meter"));
        clientRegistrationFailedMeter =  metrics.meter(name(TalkServer.class, "client-registrations-failed-meter"));
        clientLoginsSRP1SucceededMeter = metrics.meter(name(TalkServer.class, "client-logins-srp1-succeeded-meter"));
        clientLoginsSRP1FailedMeter = metrics.meter(name(TalkServer.class, "client-logins-srp1-failed-meter"));
        clientLoginsSRP2SucceededMeter = metrics.meter(name(TalkServer.class, "client-logins-srp2-succeeded-meter"));
        clientLoginsSRP2FailedMeter = metrics.meter(name(TalkServer.class, "client-logins-srp2-failed-meter"));
        messageAcceptedSucceededMeter = metrics.meter(name(TalkServer.class, "message-accepts-succeeded-meter"));
        messageConfirmedSucceededMeter = metrics.meter(name(TalkServer.class, "message-confirmations-succeeded-meter"));
        messageAcknowledgedSucceededMeter = metrics.meter(name(TalkServer.class, "message-acknowledgements-succeeded-meter"));
    }

    @Override
    public Map<String, Long> getMap() {
        HashMap<String, Long> result = new HashMap<String, Long>();
        SortedMap<String, Counter> counters = mMetrics.getCounters();
        for(Map.Entry<String, Counter> counter: counters.entrySet()) {
            result.put(counter.getKey(), counter.getValue().getCount());
        }

/*        SortedMap<String, Meter> meters = mMetrics.getMeters();
        for(Map.Entry<String, Meter> meter: meters.entrySet()) {
            result.put(meter.getKey(), meter.getValue().getOneMinuteRate());
        }*/

        return result;
    }

    @Override
    public Date getStartTime() {
        return startTime;
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
}

