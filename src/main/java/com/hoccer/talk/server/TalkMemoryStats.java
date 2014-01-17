package com.hoccer.talk.server;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory server stats
 */
public class TalkMemoryStats implements ITalkServerStatistics {

    Date startTime = new Date();

    AtomicInteger clientRegistrations = new AtomicInteger();
    AtomicInteger clientLogins = new AtomicInteger();
    AtomicInteger clientLoginsFailedSRP1 = new AtomicInteger();
    AtomicInteger clientLoginsFailedSRP2 = new AtomicInteger();

    AtomicInteger messagesAccepted = new AtomicInteger();
    AtomicInteger messagesConfirmed = new AtomicInteger();
    AtomicInteger messagesAcknowledged = new AtomicInteger();

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public Map<String, Integer> getMap() {
        HashMap<String, Integer> res = new HashMap<String, Integer>();
        res.put("clients registered", clientRegistrations.intValue());
        res.put("clients logged in", clientLogins.intValue());
        res.put("clients failed in SRP1", clientLoginsFailedSRP1.intValue());
        res.put("clients failed in SRP2", clientLoginsFailedSRP2.intValue());
        res.put("messages accepted", messagesAccepted.get());
        res.put("messages confirmed", messagesConfirmed.get());
        res.put("messages acknowledged", messagesAcknowledged.get());
        return res;
    }

    @Override
    public void countClientRegistered() {
        clientRegistrations.incrementAndGet();
    }

    @Override
    public void countClientLogin() {
        clientLogins.incrementAndGet();
    }

    @Override
    public void countClientLoginFailedSRP1() {
        clientLoginsFailedSRP1.incrementAndGet();
    }

    @Override
    public void countClientLoginFailedSRP2() {
        clientLoginsFailedSRP2.incrementAndGet();
    }

    @Override
    public void countMessageAccepted() {
        messagesAccepted.incrementAndGet();
    }

    @Override
    public void countMessageConfirmed() {
        messagesConfirmed.incrementAndGet();
    }

    @Override
    public void countMessageAcknowledged() {
        messagesAcknowledged.incrementAndGet();
    }

}
