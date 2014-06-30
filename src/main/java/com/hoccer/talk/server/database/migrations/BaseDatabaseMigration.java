package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.server.ITalkServerDatabase;

public class BaseDatabaseMigration {
    protected boolean mExecuted = false;
    protected ITalkServerDatabase mDatabase;

    public boolean isExecuted() {
        return mExecuted;
    }

    public void markAsExecuted() {
        this.mExecuted = true;
    }

    public void setDatabase(ITalkServerDatabase database) {
        this.mDatabase = database;
    }
}
