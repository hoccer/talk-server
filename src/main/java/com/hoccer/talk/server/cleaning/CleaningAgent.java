package com.hoccer.talk.server.cleaning;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CleaningAgent {

    private final static Logger LOG = Logger.getLogger(CleaningAgent.class);

    TalkServer mServer;

    ITalkServerDatabase mDatabase;

    ScheduledExecutorService mExecutor;

    public CleaningAgent(TalkServer server) {
        mServer = server;
        mDatabase = server.getDatabase();
        mExecutor = Executors.newScheduledThreadPool(4);
        cleanAllClients();
        cleanAllFinishedDeliveries();
    }

    private void cleanAllFinishedDeliveries() {
        mExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                doCleanAllFinishedDeliveries();
            }
        }, 60, 600, TimeUnit.SECONDS);
    }

    private void cleanAllClients() {
        mExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                doCleanAllClients();
            }
        }, 30, TimeUnit.SECONDS);
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
        LOG.info("cleaning all clients");
        List<TalkClient> allClients = mDatabase.findAllClients();

        for(TalkClient client: allClients) {
            cleanClientData(client.getClientId());
        }
    }

    private void doCleanAllFinishedDeliveries() {
        LOG.debug("cleaning all finished deliveries");
        List<TalkDelivery> deliveries = null;

        deliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_ABORTED);
        LOG.info("found " + deliveries.size() + " aborted deliveries");
        for(TalkDelivery delivery: deliveries) {
            doCleanFinishedDelivery(delivery);
        }
        deliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_FAILED);
        LOG.info("found " + deliveries.size() + " failed deliveries");
        for(TalkDelivery delivery: deliveries) {
            doCleanFinishedDelivery(delivery);
        }
        deliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_CONFIRMED);
        LOG.info("found " + deliveries.size() + " confirmed deliveries");
        for(TalkDelivery delivery: deliveries) {
            doCleanFinishedDelivery(delivery);
        }
    }

    private void doCleanFinishedDelivery(TalkDelivery finishedDelivery) {
        String messageId = finishedDelivery.getMessageId();
        TalkMessage message = mDatabase.findMessageById(messageId);
        if(message != null) {
            if (message.getNumDeliveries() == 1) {
                // if we have only one delivery then we can safely delete the msg now
                mDatabase.deleteMessage(message);
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
        for(TalkDelivery delivery: deliveries) {
            // confirmed and failed deliveries can always be deleted
            if(delivery.isFinished()) {
                mDatabase.deleteDelivery(delivery);
                continue;
            }
            keepMessage = true;
        }
        if(message != null && !keepMessage) {
            mDatabase.deleteMessage(message);
        }
    }

    public void cleanClientData(final String clientId) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                LOG.info("cleaning client " + clientId);
                doCleanKeysForClient(clientId);
                doCleanTokensForClient(clientId);
                doCleanRelationshipsForClient(clientId);
            }
        });
    }

    private void doCleanKeysForClient(String clientId) {
        LOG.debug("cleaning keys for client " + clientId);

        Date now = new Date();
        Calendar cal = new GregorianCalendar();
        cal.setTime(now);
        cal.add(Calendar.MONTH, -3);

        TalkPresence presence = mDatabase.findPresenceForClient(clientId);
        List<TalkKey> keys = mDatabase.findKeys(clientId);
        for(TalkKey key: keys) {
            if(key.getKeyId().equals(presence.getKeyId())) {
                LOG.debug("keeping " + key.getKeyId() + " because it is used");
                continue;
            }
            if(!cal.after(key.getTimestamp())) {
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
        int numKept = 0;
        int numSpent = 0;
        int numExpired = 0;
        List<TalkToken> tokens = mDatabase.findTokensByClient(clientId);
        for(TalkToken token: tokens) {
            if(token.getState().equals(TalkToken.STATE_SPENT)) {
                numSpent++;
                mDatabase.deleteToken(token);
                continue;
            }
            if(token.getExpiryTime().before(now)) {
                numExpired++;
                mDatabase.deleteToken(token);
                continue;
            }
        }
        if(numSpent > 0) {
            LOG.debug("deleted " + numSpent + " spent tokens");
        }
        if(numExpired > 0) {
            LOG.debug("deleted " + numExpired + " expired tokens");
        }
        if(numKept > 0) {
            LOG.debug("kept " + numKept + " tokens");
        }
    }

    private void doCleanRelationshipsForClient(String clientId) {
        LOG.debug("cleaning relationships for client " + clientId);

        Date now = new Date();
        Calendar cal = new GregorianCalendar();
        cal.setTime(now);
        cal.add(Calendar.MONTH, -3);

        List<TalkRelationship> relationships = mDatabase.findRelationshipsForClientInState(clientId, TalkRelationship.STATE_NONE);
        for(TalkRelationship relationship: relationships) {
            if(!cal.after(relationship.getLastChanged())) {
                continue;
            }
            mDatabase.deleteRelationship(relationship);
        }
    }

}
