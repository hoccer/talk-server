package com.hoccer.talk.server.database;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.j256.ormlite.dao.Dao;
import com.j256.ormlite.dao.DaoManager;
import com.j256.ormlite.jdbc.JdbcConnectionSource;
import com.j256.ormlite.table.TableUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;

/**
 * Ormlite-based database backend
 * <p/>
 * XXX WARNING broken and incomplete, use as inspiration only...
 */
public class OrmliteDatabase implements ITalkServerDatabase {

    private static final Logger LOG = Logger.getLogger(OrmliteDatabase.class);

    JdbcConnectionSource mConnectionSource;

    /* OWN SIMPLE IDENTITY */
    Dao<TalkClient, String> mClients;
    Dao<TalkMessage, String> mMessages;
    Dao<TalkPresence, String> mPresence;
    Dao<TalkGroup, String> mGroups;

    /* COMPOSITE IDENTITY */
    Dao<TalkDelivery, Long> mDeliveries;
    Dao<TalkToken, Long> mTokens;
    Dao<TalkRelationship, Long> mRelationships;
    Dao<TalkKey, Long> mKeys;
    Dao<TalkGroupMember, Long> mGroupMembers;

    public OrmliteDatabase() {
        try {
            mConnectionSource = new JdbcConnectionSource("jdbc:postgresql://localhost/talk", "talk", "talk");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            TableUtils.createTable(mConnectionSource, TalkClient.class);
            TableUtils.createTable(mConnectionSource, TalkMessage.class);
            TableUtils.createTable(mConnectionSource, TalkPresence.class);
            TableUtils.createTable(mConnectionSource, TalkGroup.class);
            TableUtils.createTable(mConnectionSource, TalkDelivery.class);
            TableUtils.createTable(mConnectionSource, TalkToken.class);
            TableUtils.createTable(mConnectionSource, TalkRelationship.class);
            TableUtils.createTable(mConnectionSource, TalkKey.class);
            TableUtils.createTable(mConnectionSource, TalkGroupMember.class);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            mClients = DaoManager.createDao(mConnectionSource, TalkClient.class);
            mMessages = DaoManager.createDao(mConnectionSource, TalkMessage.class);
            mPresence = DaoManager.createDao(mConnectionSource, TalkPresence.class);
            mGroups = DaoManager.createDao(mConnectionSource, TalkGroup.class);
            mDeliveries = DaoManager.createDao(mConnectionSource, TalkDelivery.class);
            mTokens = DaoManager.createDao(mConnectionSource, TalkToken.class);
            mRelationships = DaoManager.createDao(mConnectionSource, TalkRelationship.class);
            mKeys = DaoManager.createDao(mConnectionSource, TalkKey.class);
            mGroupMembers = DaoManager.createDao(mConnectionSource, TalkGroupMember.class);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Long> getStatistics() {
        return null;
    }

    @Override
    public List<TalkClient> findAllClients() {
        try {
            return mClients.queryForAll();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public TalkClient findClientById(String clientId) {
        try {
            return mClients.queryForId(clientId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public TalkClient findClientByApnsToken(String apnsToken) {
        try {
            return mClients.queryBuilder().where()
                    .eq("apnsToken", apnsToken)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void saveClient(TalkClient client) {
        try {
            mClients.createOrUpdate(client);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TalkMessage findMessageById(String messageId) {
        try {
            return mMessages.queryForId(messageId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    @Override
    public List<TalkMessage> findMessagesWithAttachmentFileId(String fileId) {
        return null;
    }

    @Override
    public void deleteMessage(TalkMessage message) {
        try {
            mMessages.delete(message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveMessage(TalkMessage message) {
        try {
            mMessages.createOrUpdate(message);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TalkDelivery findDelivery(String messageId, String clientId) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("messageId", messageId)
                    .and()
                    .eq("clientId", clientId)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkDelivery> findDeliveriesInState(String state) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("state", state)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    @Override
    public List<TalkDelivery> findDeliveriesInStates(String[] states) {
        return null;
    }

    @Override
    public List<TalkDelivery> findDeliveriesInStatesAndAttachmentStates(String[] deliveryStates, String[] attachmentStates) {
        return null;
    }

        @Override
    public List<TalkDelivery> findDeliveriesForMessage(String messageId) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("messageId", messageId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClient(String clientId) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("receiverId", clientId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClient(String clientId) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("senderId", clientId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("receiverId", clientId)
                    .and()
                    .eq("state", state)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    @Override
    public List<TalkDelivery> findDeliveriesForClientInDeliveryAndAttachmentStates(String clientId, String[] deliveryStates, String[] attachmentStates) {
        return null;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClientInStates(String clientId, String[] deliveryStates) {
        return null;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClientInDeliveryAndAttachmentStates(String clientId, String[] deliveryStates, String[] attachmentStates) {
        return null;
    }

    @Override
    public List<TalkDelivery> findDeliveriesFromClientInState(String clientId, String state) {
        try {
            return mDeliveries.queryBuilder().where()
                    .eq("senderId", clientId)
                    .and()
                    .eq("state", state)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteDelivery(TalkDelivery delivery) {
        try {
            mDeliveries.delete(delivery);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveDelivery(TalkDelivery delivery) {
        try {
            mDeliveries.createOrUpdate(delivery);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TalkToken> findTokensByClient(String clientId) {
        try {
            return mTokens.queryBuilder().where()
                    .eq("clientId", clientId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret) {
        try {
            return mTokens.queryBuilder().where()
                    .eq("purpose", purpose)
                    .and()
                    .eq("secret", secret)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteToken(TalkToken token) {
        try {
            mTokens.delete(token);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveToken(TalkToken token) {
        try {
            mTokens.createOrUpdate(token);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TalkPresence findPresenceForClient(String clientId) {
        try {
            return mPresence.queryForId(clientId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void savePresence(TalkPresence presence) {
        try {
            mPresence.createOrUpdate(presence);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TalkPresence> findPresencesChangedAfter(String clientId, Date lastKnown) {
        try {
            List<TalkPresence> result = new ArrayList<TalkPresence>();

            Set<String> clientsToQuery = new HashSet<String>();

            List<TalkRelationship> relationships =
                    mRelationships.queryBuilder().where()
                            .eq("otherClientId", clientId)
                            .and()
                            .eq("state", TalkRelationship.STATE_FRIEND)
                            .query();
            for (TalkRelationship relationship : relationships) {
                clientsToQuery.add(relationship.getClientId());
            }

            List<TalkGroupMember> ownMembers =
                    mGroupMembers.queryBuilder().where()
                            .eq("clientId", clientId)
                            .eq("state", TalkGroupMember.STATE_JOINED)
                            .eq("state", TalkGroupMember.STATE_INVITED)
                            .or(2)
                            .and(2)
                            .query();
            for (TalkGroupMember ownMember : ownMembers) {
                List<TalkGroupMember> otherMembers = findGroupMembersById(ownMember.getGroupId());
                for (TalkGroupMember otherMember : otherMembers) {
                    clientsToQuery.add(otherMember.getClientId());
                }
            }

            clientsToQuery.remove(clientId);

            for (String otherClient : clientsToQuery) {
                TalkPresence presence = findPresenceForClient(otherClient);
                if (presence != null) {
                    result.add(presence);
                }
            }
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public TalkKey findKey(String clientId, String keyId) {
        try {
            return mKeys.queryBuilder().where()
                    .eq("clientId", clientId)
                    .and()
                    .eq("keyId", keyId)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkKey> findKeys(String clientId) {
        try {
            return mKeys.queryForEq("clientId", clientId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteKey(TalkKey key) {
        try {
            mKeys.delete(key);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveKey(TalkKey key) {
        try {
            mKeys.createOrUpdate(key);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TalkRelationship> findRelationships(String client) {
        try {
            return mRelationships.queryBuilder().where()
                    .eq("clientId", client)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkRelationship> findRelationshipsForClientInState(String clientId, String state) {
        try {
            return mRelationships.queryBuilder().where()
                    .eq("clientId", clientId)
                    .and()
                    .eq("state", state)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkRelationship> findRelationshipsForClientInStates(String clientId, String[] states) {
        return null;
    }

    @Override
    public List<TalkRelationship> findRelationshipsByOtherClient(String other) {
        try {
            return mRelationships.queryBuilder().where()
                    .eq("otherClientId", other)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkRelationship> findRelationshipsChangedAfter(String client, Date lastKnown) {
        try {
            return mRelationships.queryBuilder().where()
                    .eq("clientId", client)
                    .and()
                    .gt("lastChanged", lastKnown)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public TalkRelationship findRelationshipBetween(String client, String otherClient) {
        try {
            return mRelationships.queryBuilder().where()
                    .eq("clientId", client)
                    .and()
                    .eq("otherClientId", otherClient)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteRelationship(TalkRelationship relationship) {
        try {
            mRelationships.delete(relationship);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveRelationship(TalkRelationship relationship) {
        try {
            mRelationships.createOrUpdate(relationship);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TalkGroup findGroupById(String groupId) {
        try {
            return mGroups.queryForId(groupId);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deleteGroup(TalkGroup group) {
        try {
            mGroups.delete(group);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TalkGroup> findGroupsByClientIdChangedAfter(String clientId, Date lastKnown) {
        List<TalkGroup> result = new ArrayList<TalkGroup>();
        List<TalkGroupMember> memberships = findGroupMembersForClient(clientId);
        for (TalkGroupMember membership : memberships) {
            result.add(findGroupById(membership.getGroupId()));
        }
        return result;
    }

    @Override
    public void saveGroup(TalkGroup group) {
        try {
            mGroups.createOrUpdate(group);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<TalkGroupMember> findGroupMembersById(String groupId) {
        try {
            return mGroupMembers.queryBuilder().where()
                    .eq("groupId", groupId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkGroupMember> findGroupMembersForClient(String clientId) {
        try {
            return mGroupMembers.queryBuilder().where()
                    .eq("clientId", clientId)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdWithStates(String groupId, String[] states) {
        return null;
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdWithStatesAndRoles(String groupId, String[] states, String [] roles) {
        return null;
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdChangedAfter(String groupId, Date lastKnown) {
        try {
            return mGroupMembers.queryBuilder().where()
                    .eq("groupId", groupId)
                    .and()
                    .gt("lastChanged", lastKnown)
                    .query();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<TalkGroupMember> findGroupMembersByIdWithStatesChangedAfter(String groupId, String[] states, Date lastKnown) {
        return null;
    }

    @Override
    public TalkGroupMember findGroupMemberForClient(String groupId, String clientId) {
        try {
            return mGroupMembers.queryBuilder().where()
                    .eq("groupId", groupId)
                    .and()
                    .eq("clientId", clientId)
                    .queryForFirst();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void saveGroupMember(TalkGroupMember groupMember) {
        try {
            mGroupMembers.createOrUpdate(groupMember);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveEnvironment(TalkEnvironment environment) {
    }

    @Override
    public TalkEnvironment findEnvironmentByClientId(String type, String clientId) {
        return null;
    }

    @Override
    public List<TalkEnvironment> findEnvironmentsForGroup(String groupId) {
        return null;
    }

    @Override
    public List<TalkEnvironment> findEnvironmentsMatching(TalkEnvironment environment) {
        return null;
    }

    @Override
    public void deleteEnvironment(TalkEnvironment environment) {
    }

    @Override
    public List<TalkGroupMember> findGroupMembersForClientWithStates(String clientId, String[] states) {
        return null;
    }

    @Override
    public boolean ping() {
        // TODO: implement me properly!
        return false;
    }

    @Override
    public void reportPing() {
        LOG.info(ping());
    }

    @Override
    public TalkClientHostInfo findClientHostInfo(String clientId) {
        // TODO: implement me properly!
        return null;
    }

    @Override
    public void saveClientHostInfo(TalkClientHostInfo clientHostInfo) {
        // TODO: implement me properly!
    }
}
