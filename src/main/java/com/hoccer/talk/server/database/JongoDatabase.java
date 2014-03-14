package com.hoccer.talk.server.database;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.WriteConcern;
import org.apache.log4j.Logger;
import org.jongo.Jongo;
import org.jongo.MongoCollection;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Database implementation using the Jongo mapper to MongoDB
 * <p/>
 * This is intended as the production backend.
 * <p/>
 * XXX this should use findOne() instead of find() where appropriate
 */
public class JongoDatabase implements ITalkServerDatabase {

    private static final Logger LOG = Logger.getLogger(JongoDatabase.class);

    /**
     * Configuration instance
     */
    private final TalkServerConfiguration mConfig;

    /**
     * Mongo connection pool
     */
    private Mongo mMongo;

    /**
     * Mongo database accessor
     */
    private DB mDb;

    /**
     * Jongo object mapper
     */
    private Jongo mJongo;

    private final List<MongoCollection> mCollections;

    private MongoCollection mClients;
    private MongoCollection mMessages;
    private MongoCollection mDeliveries;
    private MongoCollection mTokens;
    private MongoCollection mRelationships;
    private MongoCollection mPresences;
    private MongoCollection mKeys;
    private MongoCollection mGroups;
    private MongoCollection mGroupMembers;


    public JongoDatabase(TalkServerConfiguration configuration) {
        mConfig = configuration;
        mCollections = new ArrayList<MongoCollection>();
        initialize();
    }

    private void initialize() {
        String dbname = mConfig.getJongoDb();
        LOG.info("Initializing jongo with database '" + dbname + "'");

        // write concern for all collections
        // WriteConcern wc = WriteConcern.JOURNALED;

        // create connection pool
        try {
            MongoOptions options = new MongoOptions();
            options.threadsAllowedToBlockForConnectionMultiplier = 1500;
            options.maxWaitTime = 5 * 1000;
            // options.setWriteConcern(wc);
            // options.connectionsPerHost
            mMongo = new Mongo("localhost", options);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        // create db accessor
        mDb = mMongo.getDB(dbname);
        // create object mapper
        mJongo = new Jongo(mDb);

        // create collection accessors
        mClients = getCollection("client");
        mMessages = getCollection("message");
        mDeliveries = getCollection("delivery");
        mTokens = getCollection("token");
        mRelationships = getCollection("relationship");
        mPresences = getCollection("presence");
        mKeys = getCollection("key");
        mGroups = getCollection("group");
        mGroupMembers = getCollection("groupMember");
    }

    private MongoCollection getCollection(String name) {
        MongoCollection res = mJongo.getCollection(name).withWriteConcern(WriteConcern.JOURNALED);
        mCollections.add(res);
        return res;
    }

    @Override
    public Map<String, Long> getStatistics() {
        HashMap<String, Long> res = new HashMap<String, Long>();
        for (MongoCollection collection : mCollections) {
            res.put(collection.getName(), collection.count());
        }
        return res;
    }

    @Override
    public List<TalkClient> findAllClients() {
        List<TalkClient> res = new ArrayList<TalkClient>();
        for (TalkClient talkClient : mClients.find().as(TalkClient.class)) {
            res.add(talkClient);
        }
        return res;
    }

    @Override
    public TalkClient findClientById(String clientId) {
        return mClients.findOne("{clientId:#}", clientId)
                .as(TalkClient.class);
    }

    @Override
    public TalkClient findClientByApnsToken(String apnsToken) {
        return mClients.findOne("{apnsToken:#}", apnsToken)
                .as(TalkClient.class);
    }

    @Override
    public void saveClient(TalkClient client) {
        mClients.save(client);
    }

    @Override
    public TalkMessage findMessageById(String messageId) {
        return mMessages.findOne("{messageId:#}", messageId)
                .as(TalkMessage.class);
    }

    @Override
    public void deleteMessage(TalkMessage message) {
        mMessages.remove("{messageId:#}", message.getMessageId());
    }

    @Override
    public void saveMessage(TalkMessage message) {
        mMessages.save(message);
    }

    @Override
    public TalkDelivery findDelivery(String messageId, String clientId) {
        return mDeliveries.findOne("{messageId:#,receiverId:#}", messageId, clientId)
                .as(TalkDelivery.class);
    }

    @Override
    public List<TalkDelivery> findDeliveriesInState(String state) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{state:#}", state)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClient(String clientId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{receiverId:#}", clientId)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClient(String clientId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{senderId:#}", clientId)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{receiverId:#,state:#}", clientId, state)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClientInState(String clientId, String state) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{senderId:#,state:#}", clientId, state)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForMessage(String messageId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        for (TalkDelivery talkDelivery : mDeliveries.find("{messageId:#}", messageId)
                .as(TalkDelivery.class)) {
            res.add(talkDelivery);
        }
        return res;
    }

    @Override
    public void deleteDelivery(TalkDelivery delivery) {
        mDeliveries.remove("{messageId:#,receiverId:#}", delivery.getMessageId(), delivery.getReceiverId());
    }

    @Override
    public void saveDelivery(TalkDelivery delivery) {
        mDeliveries.save(delivery);
    }

    @Override
    public List<TalkToken> findTokensByClient(String clientId) {
        List<TalkToken> res = new ArrayList<TalkToken>();
        for (TalkToken talkToken : mTokens.find("{clientId:#}", clientId)
                .as(TalkToken.class)) {
            res.add(talkToken);
        }
        return res;
    }

    @Override
    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret) {
        TalkToken res = null;
        Iterator<TalkToken> it =
                mTokens.find("{purpose:#,secret:#}", purpose, secret)
                        .as(TalkToken.class).iterator();
        if (it.hasNext()) {
            res = it.next();
            if (it.hasNext()) {
                throw new RuntimeException("Duplicate token");
            }
        }
        return res;
    }

    @Override
    public void deleteToken(TalkToken token) {
        mTokens.remove("{clientId:#,secret:#}", token.getClientId(), token.getSecret());
    }

    @Override
    public void saveToken(TalkToken token) {
        mTokens.save(token);
    }

    @Override
    public TalkPresence findPresenceForClient(String clientId) {
        return mPresences.findOne("{clientId:#}", clientId)
                .as(TalkPresence.class);
    }

    @Override
    public List<TalkPresence> findPresencesChangedAfter(String clientId, Date lastKnown) {
        // result array
        List<TalkPresence> res = new ArrayList<TalkPresence>();
        // set to collect clients into
        Set<String> clients = new HashSet<String>();
        // collect clients known through relationships
        List<TalkRelationship> relationships = findRelationshipsByOtherClient(clientId);
        for (TalkRelationship relationship : relationships) {
            // if the relation is friendly
            if (relationship.isFriend()) {
                clients.add(relationship.getClientId());
            }
        }
        // collect clients known through groups
        List<TalkGroupMember> ownMembers = findGroupMembersForClient(clientId);
        for (TalkGroupMember ownMember : ownMembers) {
            String groupId = ownMember.getGroupId();
            if (ownMember.isInvited() || ownMember.isJoined()) {
                List<TalkGroupMember> otherMembers = findGroupMembersById(groupId);
                for (TalkGroupMember otherMember : otherMembers) {
                    if (otherMember.isInvited() || otherMember.isJoined()) {
                        clients.add(otherMember.getClientId());
                    }
                }
            }
        }
        // remove self
        clients.remove(clientId);
        // collect presences
        for (String client : clients) {
            TalkPresence pres = findPresenceForClient(client);
            if (pres != null) {
                if (pres.getTimestamp().after(lastKnown)) {
                    res.add(pres);
                }
            }
        }
        // return them
        return res;
    }

    @Override
    public void savePresence(TalkPresence presence) {
        mPresences.save(presence);
    }

    @Override
    public TalkKey findKey(String clientId, String keyId) {
        return mKeys.findOne("{clientId:#,keyId:#}", clientId, keyId)
                .as(TalkKey.class);
    }

    @Override
    public List<TalkKey> findKeys(String clientId) {
        List<TalkKey> res = new ArrayList<TalkKey>();
        for (TalkKey talkKey : mKeys.find("{clientId:#}", clientId)
                .as(TalkKey.class)) {
            res.add(talkKey);
        }
        return res;
    }

    @Override
    public void deleteKey(TalkKey key) {
        mKeys.remove("{clientId:#,keyId:#}", key.getClientId(), key.getKeyId());
    }

    @Override
    public void saveKey(TalkKey key) {
        mKeys.save(key);
    }

    @Override
    public List<TalkRelationship> findRelationships(String client) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        for (TalkRelationship talkRelationship : mRelationships.find("{clientId:#}", client)
                .as(TalkRelationship.class)) {
            res.add(talkRelationship);
        }
        return res;
    }

    @Override
    public List<TalkRelationship> findRelationshipsForClientInState(String clientId, String state) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        for (TalkRelationship talkRelationship : mRelationships.find("{clientId:#,state:#}", clientId, state)
                .as(TalkRelationship.class)) {
            res.add(talkRelationship);
        }
        return res;
    }

    @Override
    public List<TalkRelationship> findRelationshipsByOtherClient(String other) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        for (TalkRelationship talkRelationship : mRelationships.find("{otherClientId:#}", other)
                .as(TalkRelationship.class)) {
            res.add(talkRelationship);
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
        for (TalkRelationship talkRelationship : mRelationships.find("{clientId:#,lastChanged: {$gt:#}}", client, lastKnown)
                .as(TalkRelationship.class)) {
            res.add(talkRelationship);
        }
        return res;
    }

    @Override
    public void deleteRelationship(TalkRelationship relationship) {
        mRelationships.remove("{clientId:#,otherClientId:#}",
                relationship.getClientId(), relationship.getOtherClientId());
    }

    @Override
    public void saveRelationship(TalkRelationship relationship) {
        mRelationships.save(relationship);
    }

    @Override
    public TalkGroup findGroupById(String groupId) {
        return mGroups.findOne("{groupId:#}", groupId).as(TalkGroup.class);
    }

    @Override
    public List<TalkGroup> findGroupsByClientIdChangedAfter(String clientId, Date lastKnown) {
        // XXX dirty hack / indirect query
        List<TalkGroup> res = new ArrayList<TalkGroup>();
        List<TalkGroupMember> members = findGroupMembersForClient(clientId);
        for (TalkGroupMember member : members) {
            // String memberState = member.getState();
            if (member.isMember() || member.isInvited()) {
                TalkGroup group = findGroupById(member.getGroupId());
                //if(group.getLastChanged().after(lastKnown)) { // XXX fix this
                res.add(group);
                //}
            }
        }
        return res;
    }

    @Override
    public void saveGroup(TalkGroup group) {
        mGroups.save(group);
    }

    @Override
    public List<TalkGroupMember> findGroupMembersById(String groupId) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        for (TalkGroupMember talkGroupMember : mGroupMembers.find("{groupId:#}", groupId)
                .as(TalkGroupMember.class)) {
            res.add(talkGroupMember);
        }
        return res;
    }

    @Override
    public List<TalkGroupMember> findGroupMembersForClient(String clientId) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        for (TalkGroupMember talkGroupMember : mGroupMembers.find("{clientId:#}", clientId)
                .as(TalkGroupMember.class)) {
            res.add(talkGroupMember);
        }
        return res;
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdChangedAfter(String groupId, Date lastKnown) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        for (TalkGroupMember talkGroupMember : mGroupMembers.find("{groupId:#,lastChanged: {$gt:#}}", groupId, lastKnown)
                .as(TalkGroupMember.class)) {
            res.add(talkGroupMember);
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
