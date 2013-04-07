package com.hoccer.talk.server.database;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.mongodb.DB;
import com.mongodb.Mongo;

import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Database implementation using the Jongo mapper to MongoDB
 *
 * This is intended as the production backend.
 *
 * XXX this should use findOne() instead of find() where appropriate
 *
 */
public class JongoDatabase implements ITalkServerDatabase {

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

    public JongoDatabase() {
        initialize();
    }

    private void initialize() {
        // create connection pool
        try {
            mMongo = new Mongo();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        // create db accessor
        mDb = mMongo.getDB("talk");
        // create object mapper
        mJongo = new Jongo(mDb);
        // create collection accessors
        mClients = mJongo.getCollection("client");
        mMessages = mJongo.getCollection("message");
        mDeliveries = mJongo.getCollection("delivery");
        mTokens = mJongo.getCollection("token");
        mRelationships = mJongo.getCollection("relationship");
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
    public TalkRelationship findRelationshipBetween(String client, String otherClient) {
        TalkRelationship res = null;
        Iterator<TalkRelationship> it =
                mRelationships.find("{clientId:#,otherClientId:#}", client, otherClient)
                              .as(TalkRelationship.class).iterator();
        if(it.hasNext()) {
            res = it.next();
            if(it.hasNext()) {
                throw new RuntimeException("Multiple relationships between " + client + " and " + otherClient);
            }
        }
        return res;
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
}
