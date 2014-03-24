package com.hoccer.talk.server;

import better.jsonrpc.core.JsonRpcConnection;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Common interface for server statistics
 *
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

    com.codahale.metrics.Timer.Context signalRequestStart(JsonRpcConnection connection, ObjectNode request);

    void signalRequestStop(JsonRpcConnection connection, ObjectNode request, Timer.Context timerContext);
}
