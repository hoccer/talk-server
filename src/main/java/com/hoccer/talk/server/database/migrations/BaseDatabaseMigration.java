package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.util.NamedThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class BaseDatabaseMigration {
    protected boolean mExecuted = false;
    protected ITalkServerDatabase mDatabase;

    protected final ExecutorService mExecutor;

    private static final int THREADS_MIGRATION = 12;

    public BaseDatabaseMigration() {
        this.mExecutor = Executors.newScheduledThreadPool(
                THREADS_MIGRATION,
                new NamedThreadFactory("db-migrations")
        );
    }

    public boolean isExecuted() {
        return mExecuted;
    }

    public void markAsExecuted() {
        this.mExecuted = true;
    }

    public void setDatabase(ITalkServerDatabase database) {
        this.mDatabase = database;
    }

    public boolean isReversible() {
        return false;
    }
}
