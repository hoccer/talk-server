package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.model.TalkDatabaseMigration;
import com.hoccer.talk.server.ITalkServerDatabase;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DatabaseMigrationManager {

    private static final Logger LOG = Logger.getLogger(DatabaseMigrationManager.class);

    private final static List<Class<? extends IDatabaseMigration>> migrationClasses = new ArrayList<Class<? extends IDatabaseMigration>>();
    // List of known migrations in applicable order. This list should only ever be extended but not modified!
    // The ordering here is very important. Do not change the ordering on a whim!
    static {
        /*migrationClasses.add(DatabaseMigrationDeliveryStates.class);
        migrationClasses.add(DatabaseMigrationAttachmentStates.class);*/
    }

    private final List<IDatabaseMigration> mMigrations = new ArrayList<IDatabaseMigration>();
    private final ITalkServerDatabase mDatabase;
    private final List<TalkDatabaseMigration> mAppliedMigrations;

    public DatabaseMigrationManager(ITalkServerDatabase database) {
        this.mDatabase = database;

        mAppliedMigrations = mDatabase.findDatabaseMigrations();
        LOG.info("migrations recorded in the database:");
        for (int i1 = 0; i1 < mAppliedMigrations.size(); i1++) {
            TalkDatabaseMigration appliedMigration = mAppliedMigrations.get(i1);
            LOG.info(" * " + appliedMigration.getPosition() + ": " + appliedMigration.getName());
        }

        // check uniqueness of migration names in the loaded classes
        final List<String> migrationNames = new ArrayList<String>();

        LOG.info("known migrations:");
        for (int i = 0; i < migrationClasses.size(); i++) {
            Class<? extends IDatabaseMigration> migrationClass = migrationClasses.get(i);
            try {
                final IDatabaseMigration migration = migrationClass.newInstance();
                migration.setDatabase(mDatabase);
                if (migrationNames.contains(migration.getName())) {
                    // potentially use custom exception class here
                    throw new IllegalArgumentException("Migration with name: '" + migration.getName() + "' already exists! This is not allowed!");
                } else {
                    migrationNames.add(migration.getName());
                }
                mMigrations.add(migration);
                LOG.info(" * " + (i+1) + ": " + migration.getName());
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        validateMigrations();
    }

    public void executeAllMigrations() {
        LOG.info("executeAllMigrations - START");
        for (int i = 0; i < mMigrations.size(); i++) {
            IDatabaseMigration migration = mMigrations.get(i);
            if (migration.isExecuted()) {
                LOG.info("  * migration '" + migration.getName() + "' is already (marked as) executed - DOING NOTHING");
            } else {
                LOG.info("  * executing 'up' for migration '" + migration.getName() + "'...");
                migration.up();
                TalkDatabaseMigration dbMigration = new TalkDatabaseMigration();
                dbMigration.setName(migration.getName());
                dbMigration.setPosition((i + 1));
                mDatabase.saveDatabaseMigration(dbMigration);
                LOG.info("  * ... done executing migration '" + migration.getName() + "'");
            }
        }
        LOG.info("executeAllMigration - DONE");
    }

    private void validateMigrations() {
        LOG.info("validating migrations...");
        if (mMigrations.size() < mAppliedMigrations.size()) {
            throw new RuntimeException("There are '" + mAppliedMigrations.size() + "' migrations recorded in the database, but we only know about '" + mMigrations.size() + "'!");
        }

        // consume all migration in the db -> mark migration as already executed.
        // Order is important here
        for (int i = 0; i < mAppliedMigrations.size(); i++) {

            final IDatabaseMigration migration = mMigrations.get(i);
            if (migration == null) {
                throw new RuntimeException("database recorded migration '" + mAppliedMigrations.get(i).getName() + "' does not exist in code! - FATAL!");
            }

            if (mAppliedMigrations.get(i).getName().equals(migration.getName())) {
                LOG.debug("marking migration '" + migration.getName() + "' as executed.");
                migration.markAsExecuted();
            } else {
                throw new RuntimeException("database recorded migration '" + mAppliedMigrations.get(i).getName() + "' does not match migration with name: '" + migration.getName() + "' - Ordering mismatch? - FATAL");
            }
        }
    }
}
