package com.hoccer.talk.server.database.migrations;

import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseMigrationAttachmentStates extends BaseDatabaseMigration  implements IDatabaseMigration {

    // DO not change!
    private final static String MIGRATION_NAME= "2014_06_30_attachment_states";

    private static final Logger LOG = Logger.getLogger(DatabaseMigrationAttachmentStates.class);

    @Override
    public void up() {
        List<TalkDelivery> deliveries = mDatabase.findAllDeliveries();
        LOG.info("migrating attachment state for " + deliveries.size() + " deliveries");
        AtomicInteger deliveriesWithoutAttachmentCounter = new AtomicInteger();
        AtomicInteger deliveriesWithAttachmentsCounter = new AtomicInteger();
        for (TalkDelivery delivery : deliveries) {
            final TalkMessage message = mDatabase.findMessageById(delivery.getMessageId());
            if (message == null) {
                // Doesn't even have a message associated? Something went wrong with this delivery?
                LOG.warn("Delivery " + delivery.getId() + " has no message associated - cannot migrate attachment state");
            } else {
                if (message.getAttachmentFileId() != null) {
                    // has attachment
                    delivery.setAttachmentState(TalkDelivery.ATTACHMENT_STATE_RECEIVED_ACKNOWLEDGED);
                    mDatabase.saveDelivery(delivery);
                    deliveriesWithAttachmentsCounter.incrementAndGet();
                } else {
                    // has no attachment
                    delivery.setAttachmentState(TalkDelivery.ATTACHMENT_STATE_NONE);
                    mDatabase.saveDelivery(delivery);
                    deliveriesWithoutAttachmentCounter.incrementAndGet();
                }
            }
        }

        LOG.info("Set Attachment state to '" + TalkDelivery.ATTACHMENT_STATE_NONE + "' for " + deliveriesWithoutAttachmentCounter + " deliveries");
        LOG.info("Set Attachment state to '" + TalkDelivery.ATTACHMENT_STATE_RECEIVED_ACKNOWLEDGED + "' for " + deliveriesWithAttachmentsCounter + " deliveries");
    }

    @Override
    public void down() {

    }

    @Override
    public String getName() {
        return MIGRATION_NAME;
    }
}
