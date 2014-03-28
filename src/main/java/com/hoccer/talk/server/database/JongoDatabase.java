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
 * TODO: this should use findOne() instead of find() where appropriate
 */
public class JongoDatabase implements ITalkServerDatabase {

    private static final Logger LOG = Logger.getLogger(JongoDatabase.class);

    /**
     * Mongo connection pool
     */
    Mongo mMongo;

    /**
     * Mongo database accessor
     */
    DB mDb;

    /**
     * Jongo object mapper
     */
    Jongo mJongo;

    List<MongoCollection> mCollections;

    MongoCollection mClients;
    MongoCollection mMessages;
    MongoCollection mDeliveries;
    MongoCollection mTokens;
    MongoCollection mRelationships;
    MongoCollection mPresences;
    MongoCollection mKeys;
    MongoCollection mGroups;
    MongoCollection mGroupMembers;
    MongoCollection mEnvironments;

    public JongoDatabase(TalkServerConfiguration configuration) {
        mCollections = new ArrayList<MongoCollection>();
        mMongo = createMongoClient(configuration);
        initialize(configuration.getJongoDb());
    }

    public JongoDatabase(TalkServerConfiguration configuration, Mongo mongodb) {
        mCollections = new ArrayList<MongoCollection>();
        mMongo = mongodb;
        initialize(configuration.getJongoDb());
    }

    private Mongo createMongoClient(TalkServerConfiguration configuration) {

        // write concern for all collections
        // WriteConcern wc = WriteConcern.JOURNALED; //??? not used?
        // create connection pool
        try {
            MongoOptions options = new MongoOptions();
            options.threadsAllowedToBlockForConnectionMultiplier = 1500;
            options.maxWaitTime = 5 * 1000;
            // options.connectionsPerHost
            return new Mongo("localhost", options);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void initialize(String dbName) {
        LOG.info("Initializing jongo with database " + dbName);

        // create db accessor
        mDb = mMongo.getDB(dbName);
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
        mEnvironments = getCollection("enviroment");
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
        Iterator<TalkClient> it =
                mClients.find().as(TalkClient.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
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
        Iterator<TalkDelivery> it =
                mDeliveries.find("{state:#}", state)
                        .as(TalkDelivery.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClient(String clientId) {
        List<TalkDelivery> res = new ArrayList<TalkDelivery>();
        Iterator<TalkDelivery> it =
                mDeliveries.find("{receiverId:#}", clientId)
                        .as(TalkDelivery.class).iterator();
        while (it.hasNext()) {
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
        while (it.hasNext()) {
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
        while (it.hasNext()) {
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
        while (it.hasNext()) {
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
        while (it.hasNext()) {
            res.add(it.next());
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
        Iterator<TalkToken> it =
                mTokens.find("{clientId:#}", clientId)
                        .as(TalkToken.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
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
        Iterator<TalkKey> it =
                mKeys.find("{clientId:#}", clientId)
                        .as(TalkKey.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
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
        Iterator<TalkRelationship> it =
                mRelationships.find("{clientId:#}", client)
                        .as(TalkRelationship.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkRelationship> findRelationshipsForClientInState(String clientId, String state) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        Iterator<TalkRelationship> it =
                mRelationships.find("{clientId:#,state:#}", clientId, state)
                        .as(TalkRelationship.class).iterator();
        while (it.hasNext()) {
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
        while (it.hasNext()) {
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
        while (it.hasNext()) {
            res.add(it.next());
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
            String memberState = member.getState();
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
        Iterator<TalkGroupMember> it =
                mGroupMembers.find("{groupId:#}", groupId)
                        .as(TalkGroupMember.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    public List<TalkGroupMember> findGroupMembersByIdWithStates(String groupId, String[] states) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        Iterator<TalkGroupMember> it =
                mGroupMembers.find("{groupId:#, state: { $in: # }}", groupId, states)
                        .as(TalkGroupMember.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }


    @Override
    public List<TalkGroupMember> findGroupMembersForClient(String clientId) {
        List<TalkGroupMember> res = new ArrayList<TalkGroupMember>();
        Iterator<TalkGroupMember> it =
                mGroupMembers.find("{clientId:#}", clientId)
                        .as(TalkGroupMember.class).iterator();
        while (it.hasNext()) {
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
        while (it.hasNext()) {
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

    @Override
    public void saveEnvironment(TalkEnvironment environment) {
        mEnvironments.save(environment);
    }

    @Override
    public TalkEnvironment findEnvironmentByClientId(String clientId) {
        return mEnvironments.findOne("{clientId:#}", clientId)
                .as(TalkEnvironment.class);
    }

    @Override
    public List<TalkEnvironment> findEnvironmentsForGroup(String groupId) {
        List<TalkEnvironment> res = new ArrayList<TalkEnvironment>();
        Iterator<TalkEnvironment> it =
                mEnvironments.find("{groupId:#}", groupId)
                        .as(TalkEnvironment.class).iterator();
        while (it.hasNext()) {
            res.add(it.next());
        }
        return res;
    }

    @Override
    public List<TalkEnvironment> findEnvironmentsMatching(TalkEnvironment environment) {
        mEnvironments.ensureIndex("{geoLocation: '2dsphere'}");
        List<TalkEnvironment> res = new ArrayList<TalkEnvironment>();

        // do geospatial search
        Double[] searchCenter = environment.getGeoLocation();
        Float accuracy = environment.getAccuracy();
        if (searchCenter != null) {
            Float searchRadius = accuracy;
            if (searchRadius > 200.f) {
                searchRadius = 200.f;
            }
            if (searchRadius < 100.f) {
                searchRadius = 100.f;
            }
            Double EARTH_RADIUS = 1000.0 * 6371.0;
            Double searchRadiusRad = searchRadius / EARTH_RADIUS;
            Iterator<TalkEnvironment> it = mEnvironments.find("{ geoLocation : { $geoWithin : { $centerSphere : [ [# , #] , # ] } } }", searchCenter[0], searchCenter[1], searchRadiusRad)
                    .as(TalkEnvironment.class).iterator();
            while (it.hasNext()) {
                res.add(it.next());
            }
            LOG.debug("found " + res.size() + " environments by geolocation");
        }

        // do bssid search
        if (environment.getBssids() != null) {
            List<String> bssids = Arrays.asList(environment.getBssids());
            Iterator<TalkEnvironment> it =
                    mEnvironments.find("{ bssids :{ $in: # } }", bssids)
                            .as(TalkEnvironment.class).iterator();
            int totalFound = 0;
            int newFound = 0;
            while (it.hasNext()) {
                TalkEnvironment te = it.next();
                ++totalFound;
                boolean found = false;
                for (TalkEnvironment rte : res) {
                    if (rte.getGroupId().equals(te.getGroupId()) && rte.getClientId().equals(te.getClientId())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    res.add(te);
                    ++newFound;
                }
            }
            LOG.debug("found " + totalFound + " environments by bssid, " + newFound + " of them are new");
        }

        // do identifiers search
        if (environment.getIdentifiers() != null) {
            List<String> identifiers = Arrays.asList(environment.getIdentifiers());
            Iterator<TalkEnvironment> it =
                    mEnvironments.find("{ identifiers :{ $in: # } }", identifiers)
                            .as(TalkEnvironment.class).iterator();
            int totalFound = 0;
            int newFound = 0;
            while (it.hasNext()) {
                TalkEnvironment te = it.next();
                ++totalFound;
                boolean found = false;
                for (TalkEnvironment rte : res) {
                    if (rte.getGroupId().equals(te.getGroupId()) && rte.getClientId().equals(te.getClientId())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    res.add(te);
                    ++newFound;
                }
            }

            LOG.debug("found " + totalFound + " environments by identifiers, " + newFound + " of them are new");
        }

        return res;
    }

    @Override
    public void deleteEnvironment(TalkEnvironment environment) {
        mEnvironments.remove("{clientId:#}", environment.getClientId());
    }

    @Override
    public boolean ping() {
        return mDb.command("ping").ok();
    }

    @Override
    public void reportPing() {
        try {
            ping();
            LOG.info("Database is online");
        } catch (Exception e) {
            LOG.error("Database is not online:", e);
        }
    }
}
