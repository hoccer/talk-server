package com.hoccer.talk.server.database.migrations;

public class DatabaseMigrationAttachmentStates extends BaseDatabaseMigration  implements IDatabaseMigration {

    // DO not change!
    public final static String MIGRATION_NAME= "2014_06_30_attachment_states";

    @Override
    public void up() {
        mExecuted = true;
    }

    @Override
    public void down() {

    }

    @Override
    public String getName() {
        return MIGRATION_NAME;
    }

    @Override
    public boolean isReversible() {
        return false;
    }
}
