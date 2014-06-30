package com.hoccer.talk.server.database.migrations;

public class DatabaseMigrationDeliveryStates extends BaseDatabaseMigration implements IDatabaseMigration {

    // DO not change!
    public final static String MIGRATION_NAME= "2014_06_30_delivery_states";

    @Override
    public void up() {
        // TODO: implement me (mDatabase is availble!)
        markAsExecuted();
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
