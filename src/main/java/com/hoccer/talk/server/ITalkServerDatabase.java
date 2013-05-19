package com.hoccer.talk.server;

import com.hoccer.talk.model.*;

import java.util.Date;
import java.util.List;

/**
 * Describes the interface of Talk database backends
 *
 * There currently are two implementations:
 *
 *   .database.JongoDatabase   -  Jongo-based persistent database
 *
 *   .database.MemoryDatabase  -  Hashtable-based in-memory database
 *
 */
public interface ITalkServerDatabase {

    public TalkClient findClientById(String clientId);
    public TalkClient findClientByApnsToken(String apnsToken);
    public void saveClient(TalkClient client);

    public TalkMessage findMessageById(String messageId);
    public void saveMessage(TalkMessage message);

    public TalkDelivery findDelivery(String messageId, String clientId);
    public List<TalkDelivery> findDeliveriesForClient(String clientId);
    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state);
    public List<TalkDelivery> findDeliveriesFromClient(String clientId);
    public List<TalkDelivery> findDeliveriesFromClientInState(String clientId, String state);
    public List<TalkDelivery> findDeliveriesForMessage(String messageId);
    public void saveDelivery(TalkDelivery delivery);

    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret);
    public void saveToken(TalkToken token);

    public TalkPresence findPresenceForClient(String clientId);
    public void savePresence(TalkPresence presence);
    public List<TalkPresence> findPresencesChangedAfter(String clientId, Date lastKnown);

    public TalkKey findKey(String clientId, String keyId);
    public void saveKey(TalkKey key);

    public List<TalkRelationship> findRelationships(String client);
    public List<TalkRelationship> findRelationshipsByOtherClient(String other);
    public List<TalkRelationship> findRelationshipsChangedAfter(String client, Date lastKnown);
    public TalkRelationship findRelationshipBetween(String client, String otherClient);
    public void saveRelationship(TalkRelationship relationship);

    public TalkGroup findGroupById(String groupId);
    public List<TalkGroup> findGroupsByClientIdChangedAfter(String clientId, Date lastKnown);
    public void saveGroup(TalkGroup group);

    public List<TalkGroupMember> findGroupMembersById(String groupId);
    public List<TalkGroupMember> findGroupMembersForClient(String clientId);
    public List<TalkGroupMember> findGroupMembersByIdChangedAfter(String groupId, Date lastKnown);
    public TalkGroupMember findGroupMemberForClient(String groupId, String clientId);
    public void saveGroupMember(TalkGroupMember groupMember);

}
