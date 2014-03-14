package com.hoccer.talk.server;

import java.util.Date;
import java.util.Map;

/**
 * Common interface for server statistics
 * <p/>
 * This is here so the crappy in-memory statistics
 * can be replaced with Metrics easily.
 */
public interface ITalkServerStatistics {

    void signalClientRegisteredSucceeded();

    void signalClientRegisteredFailed();

    void signalClientLoginSRP1Succeeded();

    void signalClientLoginSRP1Failed();

    void signalClientLoginSRP2Succeeded();

    void signalClientLoginSRP2Failed();

    void signalMessageAcceptedSucceeded();

    void signalMessageConfirmedSucceeded();

    void signalMessageAcknowledgedSucceeded();

    public Date getStartTime();

    public Map<String, Long> getMap();

}
