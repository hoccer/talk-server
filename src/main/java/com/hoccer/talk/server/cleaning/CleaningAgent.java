package com.hoccer.talk.server.cleaning;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.filecache.FilecacheClient;
import com.hoccer.talk.util.NamedThreadFactory;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.*;

/**
 * Cleaning agent
 * <p/>
 * This agent takes care of database garbage. We need it for several
 * reasons, the most important being multi-destination delivery.
 * <p/>
 * It is even more justified on mongodb where there is no transactionality.
 * <p/>
 * The various parts of it run in background and in other beneficial situations.
 */
public class CleaningAgent {

    private final static Logger LOG = Logger.getLogger(CleaningAgent.class);

    @SuppressWarnings("FieldCanBeLocal")
    private final TalkServer mServer;
    private final TalkServerConfiguration mConfig;
    private final ITalkServerDatabase mDatabase;
    private final ScheduledExecutorService mExecutor;

    private static final int KEY_LIFE_TIME = 3; // in months
    private static final int RELATIONSHIP_LIFE_TIME = 3; // in months

    public CleaningAgent(TalkServer server) {
        mServer = server;
        mConfig = mServer.getConfiguration();
        mDatabase = mServer.getDatabase();
        mExecutor = Executors.newScheduledThreadPool(
            TalkServerConfiguration.THREADS_CLEANING,
            new NamedThreadFactory("cleaning-agent")
        );
        LOG.info("Cleaning clients scheduling will start in '" + mConfig.getCleanupAllClientsDelay() + "' seconds.");
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                scheduleCleanAllClients();
            }
        }, mConfig.getCleanupAllClientsDelay(), TimeUnit.SECONDS);
        LOG.info("Cleaning deliveries scheduling will start in '" + mConfig.getCleanupAllDeliveriesDelay() + "' seconds.");
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                scheduleCleanAllDeliveries();
            }
        }, mConfig.getCleanupAllDeliveriesDelay(), TimeUnit.SECONDS);

    }

    // TODO: Also clean groups (normal and nearby)

    public void cleanClientData(final String clientId) {
        LOG.debug("cleaning client " + clientId);
        doCleanKeysForClient(clientId);
        doCleanTokensForClient(clientId);
        doCleanRelationshipsForClient(clientId);
    }

    private void scheduleCleanAllDeliveries() {
        LOG.info("scheduling deliveries cleanup in '" + mConfig.getCleanupAllDeliveriesInterval() + "' seconds.");
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                doCleanAllFinishedDeliveries();
            }
        }, mConfig.getCleanupAllDeliveriesInterval(), TimeUnit.SECONDS);
    }

    private void scheduleCleanAllClients() {
        LOG.info("scheduling client cleanup in '" + mConfig.getCleanupAllClientsInterval() + "' seconds.");
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                doCleanAllClients();
            }
        }, mConfig.getCleanupAllClientsInterval(), TimeUnit.SECONDS);
    }

    public void cleanFinishedDelivery(final TalkDelivery finishedDelivery) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                doCleanFinishedDelivery(finishedDelivery);
            }
        });
    }

    private void doCleanAllClients() {
        long startTime = System.currentTimeMillis();
        LOG.info("Cleaning all clients. Determining list of clients...");
        List<TalkClient> allClients = mDatabase.findAllClients();
        LOG.info("Cleaning '" + allClients.size() + "' clients...");
        for (TalkClient client : allClients) {
            cleanClientData(client.getClientId());
        }
        long endTime = System.currentTimeMillis();
        LOG.info("Cleaning of '" + allClients.size() + "' clients done (took '" + (endTime - startTime) + "ms'). rescheduling next run...");
        scheduleCleanAllClients();
    }

    private void doCleanAllFinishedDeliveries() {
        long startTime = System.currentTimeMillis();
        LOG.info("Cleaning all finished deliveries...");

        List<TalkDelivery> finishedDeliveries = mDatabase.findDeliveriesInStatesAndAttachmentStates(TalkDelivery.FINAL_STATES, TalkDelivery.FINAL_ATTACHMENT_STATES);
        if (!finishedDeliveries.isEmpty()) {
            LOG.info("cleanup found " + finishedDeliveries.size() + " finished deliveries");
            for (TalkDelivery delivery : finishedDeliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        int totalDeliveriesCleaned = finishedDeliveries.size();

        /*
        List<TalkDelivery> abortedDeliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_ABORTED);
        if (!abortedDeliveries.isEmpty()) {
            LOG.info("cleanup found " + abortedDeliveries.size() + " aborted deliveries");
            for (TalkDelivery delivery : abortedDeliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        List<TalkDelivery> failedDeliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_FAILED);
        if (!failedDeliveries.isEmpty()) {
            LOG.info("cleanup found " + failedDeliveries.size() + " failed deliveries");
            for (TalkDelivery delivery : failedDeliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        List<TalkDelivery> confirmedDeliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_DELIVERED_ACKNOWLEDGED);
        if (!confirmedDeliveries.isEmpty()) {
            LOG.info("cleanup found " + confirmedDeliveries.size() + " confirmed deliveries");
            for (TalkDelivery delivery : confirmedDeliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        int totalDeliveriesCleaned = abortedDeliveries.size() + failedDeliveries.size() + confirmedDeliveries.size();
        */
        long endTime = System.currentTimeMillis();
        LOG.info("Cleaning of '" + totalDeliveriesCleaned + "' deliveries done (took '" + (endTime - startTime) + "ms'). rescheduling next run...");
    }

    private void doCleanFinishedDelivery(TalkDelivery finishedDelivery) {
        String messageId = finishedDelivery.getMessageId();
        TalkMessage message = mDatabase.findMessageById(messageId);
        if (message != null) {
            if (message.getNumDeliveries() == 1) {
                // if we have only one delivery then we can safely delete the msg now
                doDeleteMessage(message);
            } else {
                // else we need to determine the state of the message in detail
                doCleanDeliveriesForMessage(messageId, message);
            }
        } else {
            doCleanDeliveriesForMessage(messageId, null);
        }
        // always delete the ACKed delivery
        mDatabase.deleteDelivery(finishedDelivery);
    }

    private void doCleanDeliveriesForMessage(String messageId, TalkMessage message) {
        boolean keepMessage = false;
        List<TalkDelivery> deliveries = mDatabase.findDeliveriesForMessage(messageId);
        for (TalkDelivery delivery : deliveries) {
            // confirmed and failed deliveries can always be deleted
            if (delivery.isFinished()) {
                mDatabase.deleteDelivery(delivery);
                continue;
            }
            keepMessage = true;
        }
        if (message != null && !keepMessage) {
            doDeleteMessage(message);
        }
    }

    private void doCleanKeysForClient(String clientId) {
        LOG.debug("cleaning keys for client " + clientId);

        Date now = new Date();
        Calendar cal = new GregorianCalendar();
        cal.setTime(now);
        cal.add(Calendar.MONTH, -KEY_LIFE_TIME);

        TalkPresence presence = mDatabase.findPresenceForClient(clientId);
        List<TalkKey> keys = mDatabase.findKeys(clientId);
        for (TalkKey key : keys) {
            if (key.getKeyId().equals(presence.getKeyId())) {
                LOG.debug("keeping " + key.getKeyId() + " because it is used");
                continue;
            }
            if (!cal.after(key.getTimestamp())) {
                LOG.debug("keeping " + key.getKeyId() + " because it is recent");
                continue;
            }
            LOG.debug("deleting key " + key.getKeyId());
            mDatabase.deleteKey(key);
        }
    }

    private void doCleanTokensForClient(String clientId) {
        LOG.debug("cleaning tokens for client " + clientId);

        Date now = new Date();
        int numSpent = 0;
        int numExpired = 0;
        List<TalkToken> tokens = mDatabase.findTokensByClient(clientId);
        for (TalkToken token : tokens) {
            if (token.getState().equals(TalkToken.STATE_SPENT)) {
                numSpent++;
                mDatabase.deleteToken(token);
                continue;
            }
            if (token.getExpiryTime().before(now)) {
                numExpired++;
                mDatabase.deleteToken(token);
            }
        }
        if (numSpent > 0) {
            LOG.debug("deleted " + numSpent + " spent tokens");
        }
        if (numExpired > 0) {
            LOG.debug("deleted " + numExpired + " expired tokens");
        }
    }

    private void doCleanRelationshipsForClient(String clientId) {
        LOG.debug("cleaning relationships for client " + clientId);

        Date now = new Date();
        Calendar cal = new GregorianCalendar();
        cal.setTime(now);
        cal.add(Calendar.MONTH, -RELATIONSHIP_LIFE_TIME);

        List<TalkRelationship> relationships = mDatabase.findRelationshipsForClientInState(clientId, TalkRelationship.STATE_NONE);
        for (TalkRelationship relationship : relationships) {
            if (!cal.after(relationship.getLastChanged())) {
                continue;
            }
            mDatabase.deleteRelationship(relationship);
            LOG.debug("deleted relationship from clientId '" + relationship.getClientId() + "' to clientID '" + relationship.getOtherClientId() + "'");
        }
    }

    private void doDeleteMessage(TalkMessage message) {
        LOG.debug("deleting message " + message);

        // delete attached file if there is one
        String fileId = message.getAttachmentFileId();
        if (fileId != null) {
            FilecacheClient filecache = mServer.getFilecacheClient();
            if (filecache == null) {
                throw new RuntimeException("cant get filecache");
            }
            filecache.deleteFile(fileId);
        }

        // delete the message itself
        mDatabase.deleteMessage(message);
    }

}
