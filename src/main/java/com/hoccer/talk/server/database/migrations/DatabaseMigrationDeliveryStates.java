package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.model.TalkDelivery;
import org.apache.log4j.Logger;

import java.util.List;

public class DatabaseMigrationDeliveryStates extends BaseDatabaseMigration implements IDatabaseMigration {

    // DO not change!
    private final static String MIGRATION_NAME= "2014_06_30_delivery_states";

    private final static Logger LOG = Logger.getLogger(DatabaseMigrationDeliveryStates.class);

    @Override
    public void up() {
        migrateFromDeliveriesFromStateToState(TalkDelivery.STATE_DELIVERED_OLD, TalkDelivery.STATE_DELIVERED_PRIVATE);
        migrateFromDeliveriesFromStateToState(TalkDelivery.STATE_CONFIRMED_OLD, TalkDelivery.STATE_DELIVERED_PRIVATE_ACKNOWLEDGED);
        migrateFromDeliveriesFromStateToState(TalkDelivery.STATE_ABORTED_OLD, TalkDelivery.STATE_ABORTED_ACKNOWLEDGED);
        migrateFromDeliveriesFromStateToState(TalkDelivery.STATE_FAILED_OLD, TalkDelivery.STATE_FAILED_ACKNOWLEDGED);
    }

    @Override
    public void down() {

    }

    @Override
    public String getName() {
        return MIGRATION_NAME;
    }

    private void migrateFromDeliveriesFromStateToState(String startState, String targetState) {
        final List<TalkDelivery> deliveries = mDatabase.findDeliveriesInState(startState);
        for (TalkDelivery delivery : deliveries) {
            delivery.setState(targetState);
            mDatabase.saveDelivery(delivery);
        }
        LOG.info("migrated state for " + deliveries.size() + " deliveries from: '" + startState + "' to '" + targetState + "'");
    }
}
