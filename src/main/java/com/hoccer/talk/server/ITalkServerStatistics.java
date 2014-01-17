package com.hoccer.talk.server;

import java.util.Date;
import java.util.Map;

/**
 * Common interface for server statistics
 *
 * This is here so the crappy in-memory statistics
 * can be replaced with Metrics easily.
 */
public interface ITalkServerStatistics {

    void countClientRegistered();
    void countClientLogin();
    void countClientLoginFailedSRP1();
    void countClientLoginFailedSRP2();

    void countMessageAccepted();
    void countMessageConfirmed();
    void countMessageAcknowledged();

    public Date getStartTime();
    public Map<String, Integer> getMap();

}
