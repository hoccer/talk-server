package com.hoccer.talk.server.cleaning;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkKey;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.model.TalkRelationship;
import com.hoccer.talk.model.TalkToken;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.filecache.FilecacheClient;
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

    TalkServerConfiguration mConfig;

    ITalkServerDatabase mDatabase;

    FilecacheClient mFilecache;

    ScheduledExecutorService mExecutor;

    public CleaningAgent(TalkServer server) {
        mServer = server;
        mConfig = server.getConfiguration();
        mDatabase = server.getDatabase();
        mFilecache = server.getFilecacheClient();
        mExecutor = Executors.newScheduledThreadPool(4);
        scheduleCleanAllClients();
        scheduleCleanAllDeliveries();
    }

    public void cleanClientData(final String clientId) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                LOG.debug("cleaning client " + clientId);
                doCleanKeysForClient(clientId);
                doCleanTokensForClient(clientId);
                doCleanRelationshipsForClient(clientId);
            }
        });
    }

    private void scheduleCleanAllDeliveries() {
        mExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                doCleanAllFinishedDeliveries();
            }
        }, mConfig.getCleanupAllDeliveriesDelay(),
           mConfig.getCleanupAllDeliveriesInterval(),
           TimeUnit.SECONDS);
    }

    private void scheduleCleanAllClients() {
        mExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                doCleanAllClients();
            }
        }, mConfig.getCleanupAllClientsDelay(),
                mConfig.getCleanupAllClientsInterval(),
                TimeUnit.SECONDS);
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
        if(!deliveries.isEmpty()) {
            LOG.info("cleanup found " + deliveries.size() + " aborted deliveries");
            for(TalkDelivery delivery: deliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        deliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_FAILED);
        if(!deliveries.isEmpty()) {
            LOG.info("cleanup found " + deliveries.size() + " failed deliveries");
            for(TalkDelivery delivery: deliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
        deliveries = mDatabase.findDeliveriesInState(TalkDelivery.STATE_CONFIRMED);
        if(!deliveries.isEmpty()) {
            LOG.info("cleanup found " + deliveries.size() + " confirmed deliveries");
            for(TalkDelivery delivery: deliveries) {
                doCleanFinishedDelivery(delivery);
            }
        }
    }

    private void doCleanFinishedDelivery(TalkDelivery finishedDelivery) {
        String messageId = finishedDelivery.getMessageId();
        TalkMessage message = mDatabase.findMessageById(messageId);
        if(message != null) {
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
        for(TalkDelivery delivery: deliveries) {
            // confirmed and failed deliveries can always be deleted
            if(delivery.isFinished()) {
                mDatabase.deleteDelivery(delivery);
                continue;
            }
            keepMessage = true;
        }
        if(message != null && !keepMessage) {
            doDeleteMessage(message);
        }
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

    private void doDeleteMessage(TalkMessage message) {
        LOG.debug("deleting message " + message);

        // delete attached file if there is one
        String fileId = message.getAttachmentFileId();
        if(fileId != null) {
            mFilecache.deleteFile(fileId);
        }

        // delete the message itself
        mDatabase.deleteMessage(message);
    }

}
