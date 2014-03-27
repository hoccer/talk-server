package com.hoccer.talk.server.database;

import com.codahale.metrics.health.HealthCheck;
import com.hoccer.talk.server.ITalkServerDatabase;

public class DatabaseHealthCheck extends HealthCheck {

    private final ITalkServerDatabase mDatabase;

    public DatabaseHealthCheck(ITalkServerDatabase database) {
        mDatabase = database;
    }

    @Override
    protected Result check() throws Exception {
        if (mDatabase.ping()) {
            return Result.healthy();
        }
        return Result.unhealthy("ping failed");
    }
}
