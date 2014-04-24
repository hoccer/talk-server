package com.hoccer.talk.server;

import com.hoccer.talk.model.*;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Describes the interface of Talk database backends
 * <p/>
 * There currently are two implementations:
 * <p/>
 * .database.JongoDatabase    -  Jongo-based persistent database
 * .database.OrmLiteDatabase  -  Classical Relational persistent database (e.g. Postgresql) - currently unfinished
 */
public interface ITalkServerDatabase {

    public Map<String, Long> getStatistics();

    public List<TalkClient> findAllClients();

    public TalkClient findClientById(String clientId);

    public TalkClient findClientByApnsToken(String apnsToken);

    public void saveClient(TalkClient client);

    public TalkMessage findMessageById(String messageId);

    public void deleteMessage(TalkMessage message);

    public void saveMessage(TalkMessage message);

    public TalkDelivery findDelivery(String messageId, String clientId);

    public List<TalkDelivery> findDeliveriesInState(String state);

    public List<TalkDelivery> findDeliveriesForClient(String clientId);

    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state);

    public List<TalkDelivery> findDeliveriesFromClient(String clientId);

    public List<TalkDelivery> findDeliveriesFromClientInState(String clientId, String state);

    public List<TalkDelivery> findDeliveriesForMessage(String messageId);

    public void deleteDelivery(TalkDelivery delivery);

    public void saveDelivery(TalkDelivery delivery);

    public List<TalkToken> findTokensByClient(String clientId);

    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret);

    public void deleteToken(TalkToken token);

    public void saveToken(TalkToken token);

    public TalkPresence findPresenceForClient(String clientId);

    public void savePresence(TalkPresence presence);

    public List<TalkPresence> findPresencesChangedAfter(String clientId, Date lastKnown);

    public TalkKey findKey(String clientId, String keyId);

    public List<TalkKey> findKeys(String clientId);

    public void deleteKey(TalkKey key);

    public void saveKey(TalkKey key);

    public List<TalkRelationship> findRelationships(String client);

    public List<TalkRelationship> findRelationshipsForClientInState(String clientId, String state);

    public List<TalkRelationship> findRelationshipsByOtherClient(String other);

    public List<TalkRelationship> findRelationshipsChangedAfter(String client, Date lastKnown);

    public TalkRelationship findRelationshipBetween(String client, String otherClient);

    public void deleteRelationship(TalkRelationship relationship);

    public void saveRelationship(TalkRelationship relationship);

    public TalkGroup findGroupById(String groupId);

    public List<TalkGroup> findGroupsByClientIdChangedAfter(String clientId, Date lastKnown);

    public void saveGroup(TalkGroup group);

    public List<TalkGroupMember> findGroupMembersById(String groupId);

    public List<TalkGroupMember> findGroupMembersByIdWithStates(String groupId, String[] states);

    public List<TalkGroupMember> findGroupMembersByIdChangedAfter(String groupId, Date lastKnown);

    public List<TalkGroupMember> findGroupMembersForClient(String clientId);

    public List<TalkGroupMember> findGroupMembersForClientWithStates(String clientId, String[] states);

    public TalkGroupMember findGroupMemberForClient(String groupId, String clientId);

    public void saveGroupMember(TalkGroupMember groupMember);

    public void saveEnvironment(TalkEnvironment environment);

    public TalkEnvironment findEnvironmentByClientId(String type, String clientId);

    public List<TalkEnvironment> findEnvironmentsForGroup(String groupId);

    public List<TalkEnvironment> findEnvironmentsMatching(TalkEnvironment environment);

    public void deleteEnvironment(TalkEnvironment environment);

    public boolean ping();

    public void reportPing();

    public boolean acquireGroupKeyUpdateLock(String groupId, String lockingClientId);

    public void releaseGroupKeyUpdateLock(String groupId);
}
