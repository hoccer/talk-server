package com.hoccer.talk.server;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

public class TalkMetricStats implements ITalkServerStatistics {

    Date startTime = new Date();

    MetricRegistry mMetrics;
    private final Counter clientRegistrations;
    private final Counter clientLogins;
    private final Counter clientLoginsFailedSRP1;
    private final Counter clientLoginsFailedSRP2;
    private final Counter messagesAccepted;
    private final Counter messagesConfirmed;
    private final Counter messagesAcknowledged;


    public TalkMetricStats(MetricRegistry metrics) {
        mMetrics = metrics;

        // Counters
        clientRegistrations = metrics.counter(name(TalkServer.class, "client-registrations"));

        clientLogins = metrics.counter(name(TalkServer.class, "client-logins"));
        clientLoginsFailedSRP1 = metrics.counter(name(TalkServer.class, "client-logins-failed-srp1"));
        clientLoginsFailedSRP2 = metrics.counter(name(TalkServer.class, "client-logins-failed-srp2"));

        messagesAccepted = metrics.counter(name(TalkServer.class, "messages-accepted"));
        messagesConfirmed = metrics.counter(name(TalkServer.class, "messages-confirmed"));
        messagesAcknowledged = metrics.counter(name(TalkServer.class, "messages-acknowledged"));

        // Meters
    }

    @Override
    public Map<String, Long> getMap() {
        HashMap<String, Long> result = new HashMap<String, Long>();
        result.put("clients registered", clientRegistrations.getCount());
        result.put("clients logged in", clientLogins.getCount());
        result.put("clients failed in SRP1", clientLoginsFailedSRP1.getCount());
        result.put("clients failed in SRP2", clientLoginsFailedSRP2.getCount());
        result.put("messages accepted", messagesAccepted.getCount());
        result.put("messages confirmed", messagesConfirmed.getCount());
        result.put("messages acknowledged", messagesAcknowledged.getCount());
        return result;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public void countClientRegistered() {
        clientRegistrations.inc();
    }

    @Override
    public void countClientLogin() {
        clientLogins.inc();
    }

    @Override
    public void countClientLoginFailedSRP1() {
        clientLoginsFailedSRP1.inc();
    }

    @Override
    public void countClientLoginFailedSRP2() {
        clientLoginsFailedSRP2.inc();
    }

    @Override
    public void countMessageAccepted() {
        messagesAccepted.inc();
    }

    @Override
    public void countMessageConfirmed() {
        messagesConfirmed.inc();
    }

    @Override
    public void countMessageAcknowledged() {
        messagesAcknowledged.inc();
    }
}

