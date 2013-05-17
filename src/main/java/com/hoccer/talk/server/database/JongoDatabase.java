package com.hoccer.talk.server.database;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.mongodb.DB;
import com.mongodb.Mongo;

import com.mongodb.WriteConcern;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Database implementation using the Jongo mapper to MongoDB
 *
 * This is intended as the production backend.
 *
 * XXX this should use findOne() instead of find() where appropriate
 *
 */
public class JongoDatabase implements ITalkServerDatabase {

    private static final Logger LOG = HoccerLoggers.getLogger(JongoDatabase.class);

    /** Configuration instance */
    TalkServerConfiguration mConfig;

    /** Mongo connection pool */
    Mongo mMongo;

    /** Mongo database accessor */
    DB mDb;

    /** Jongo object mapper */
    Jongo mJongo;

    MongoCollection mClients;
    MongoCollection mMessages;
    MongoCollection mDeliveries;
    MongoCollection mTokens;
    MongoCollection mRelationships;
    MongoCollection mPresences;
    MongoCollection mKeys;
    MongoCollection mGroupMembers;


    public JongoDatabase(TalkServerConfiguration configuration) {
        mConfig = configuration;
        initialize();
    }

    private void initialize() {
        String dbname = mConfig.getJongoDb();
        LOG.info("Initializing jongo with database " + dbname);

        // write concern for all collections
        WriteConcern wc = WriteConcern.JOURNALED;

        // create connection pool
        try {
            mMongo = new Mongo();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        // create db accessor
        mDb = mMongo.getDB(dbname);
        // create object mapper
        mJongo = new Jongo(mDb);
        // create collection accessors
        mClients = mJongo.getCollection("client").withWriteConcern(wc);
        mMessages = mJongo.getCollection("message").withWriteConcern(wc);
        mDeliveries = mJongo.getCollection("delivery").withWriteConcern(wc);
        mTokens = mJongo.getCollection("token").withWriteConcern(wc);
        mRelationships = mJongo.getCollection("relationship").withWriteConcern(wc);
        mPresences = mJongo.getCollection("presence").withWriteConcern(wc);
        mKeys = mJongo.getCollection("key").withWriteConcern(wc);
        mGroupMembers = mJongo.getCollection("groupMember").withWriteConcern(wc);
    }

    @Override
    public TalkClient findClientById(String clientId) {
        Iterator<TalkClient> it =
                mClients.find("{clientId:#}", clientId)
                        .as(TalkClient.class).iterator();
        if(it.hasNext()) {
            return it.next();
        } else {
            // XXX gross hack
            TalkClient newClient = new TalkClient();
            newClient.setClientId(clientId);
            saveClient(newClient);
            return newClient;
        }
    }

    @Override
    public TalkClient findClientByApnsToken(String apnsToken) {
        Iterator<TalkClient> it =
                mClients.find("{apnsToken:#}", apnsToken)
                        .as(TalkClient.class).iterator();
        if(it.hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    @Override
    public void saveClient(TalkClient client) {
        mClients.save(client);
    }

    @Override
    public TalkMessage findMessageById(String messageId) {
        Iterator<TalkMessage> it =
                mMessages.find("{messageId:#}", messageId)
                         .as(TalkMessage.class).iterator();
        if(it.hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    @Override
    public void saveMessage(TalkMessage message) {
        mMessages.save(message);
    }

    @Override
    public TalkDelivery findDelivery(String messageId, String clientId) {
        Iterator<TalkDelivery> it =
                mDeliveries.find("{messageId:#,receiverId:#}", messageId, clientId)
                           .as(TalkDelivery.class).iterator();
        if(it.hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClient(String clientId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{receiverId:#}", clientId)
                           .as(TalkDelivery.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClient(String clientId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{senderId:#}", clientId)
                        .as(TalkDelivery.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{receiverId:#,state:#}", clientId, state)
                        .as(TalkDelivery.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClientInState(String clientId, String state) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{senderId:#,state:#}", clientId, state)
                        .as(TalkDelivery.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForMessage(String messageId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{messageId:#}", messageId)
                           .as(TalkDelivery.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public void saveDelivery(TalkDelivery delivery) {
        mDeliveries.save(delivery);
    }

    @Override
    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret) {
        TalkToken res = null;
        Iterator<TalkToken> it =
                mTokens.find("{purpose:#,secret:#}", purpose, secret)
                       .as(TalkToken.class).iterator();
        if(it.hasNext()) {
            res = it.next();
            if(it.hasNext()) {
                throw new RuntimeException("Duplicate token");
            }
        }
        return res;
    }

    @Override
    public void saveToken(TalkToken token) {
        mTokens.save(token);
    }

    @Override
    public TalkPresence findPresenceForClient(String clientId) {
        return mPresences.findOne("{clientId:#}", clientId).as(TalkPresence.class);
    }

    @Override
    public List<TalkPresence> findPresencesChangedAfter(String clientId, Date lastKnown) {
        List<TalkPresence> res = new ArrayList<TalkPresence>();
        List<TalkRelationship> rels = findRelationships(clientId);
        for(TalkRelationship rel: rels) {
            if(rel.getState().equals(TalkRelationship.STATE_FRIEND)) {
                TalkPresence pres = findPresenceForClient(rel.getOtherClientId());
                if(pres != null) {
                    if(pres.getTimestamp().after(lastKnown)) {
                        res.add(pres);
                    }
                }
            }
        }
        return res;
    }

    @Override
    public void savePresence(TalkPresence presence) {
        mPresences.save(presence);
    }

    @Override
    public TalkKey findKey(String clientId, String keyId) {
        return mKeys.findOne("{clientId:#,keyId:#}", clientId, keyId).as(TalkKey.class);
    }

    @Override
    public void saveKey(TalkKey key) {
        mKeys.save(key);
    }

    @Override
    public List<TalkRelationship> findRelationships(String client) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        Iterator<TalkRelationship> it =
                mRelationships.find("{clientId:#}", client)
                              .as(TalkRelationship.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkRelationship> findRelationshipsByOtherClient(String other) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        Iterator<TalkRelationship> it =
                mRelationships.find("{otherClientId:#}", other)
                        .as(TalkRelationship.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public TalkRelationship findRelationshipBetween(String client, String otherClient) {
        return mRelationships.findOne("{clientId:#,otherClientId:#}", client, otherClient)
                              .as(TalkRelationship.class);
    }

    @Override
    public List<TalkRelationship> findRelationshipsChangedAfter(String client, Date lastKnown) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        Iterator<TalkRelationship> it =
                mRelationships.find("{clientId:#,lastChanged: {$gt:#}}", client, lastKnown)
                              .as(TalkRelationship.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public void saveRelationship(TalkRelationship relationship) {
        mRelationships.save(relationship);
    }

    @Override
    public List<TalkGroupMember> findGroupMembersById(String groupId) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        Iterator<TalkGroupMember> it =
                mGroupMembers.find("{groupId:#}", groupId)
                                .as(TalkGroupMember.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdChangedAfter(String groupId, Date lastKnown) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        Iterator<TalkGroupMember> it =
                mGroupMembers.find("{groupId:#,lastChanged: {$gt:#}}", groupId, lastKnown)
                        .as(TalkGroupMember.class).iterator();
        while(it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public TalkGroupMember findGroupMemberForClient(String groupId, String clientId) {
        return mGroupMembers.findOne("{groupId:#,clientId:#}", groupId, clientId)
                             .as(TalkGroupMember.class);
    }

    @Override
    public void saveGroupMember(TalkGroupMember groupMember) {
        mGroupMembers.save(groupMember);
    }

}
