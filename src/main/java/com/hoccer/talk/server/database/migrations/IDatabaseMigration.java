package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.server.ITalkServerDatabase;

interface IDatabaseMigration {

    /* up contains the actual code to bring the database into the desired state. Theoretically anything goes in here */
    public void up();

    /* Architecturally it is important that migrations should be reversible in principle. However this may not alway
    * be the case, depending on the migration. If it is reversible, implement these methods in an appropriate fashion*/
    public void down();
    public boolean isReversible();

    /* Marker if a migration is executed as marked in the database.*/
    public boolean isExecuted();
    public void markAsExecuted();

    /*The migration itself requires access to the db instance to actually do something. Please note that the TalkServer
    * is not created yet at this moment.*/
    public void setDatabase(ITalkServerDatabase database);

    String getName();
}