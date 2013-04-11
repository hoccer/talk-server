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
    public void saveClient(TalkClient client);

    public TalkMessage findMessageById(String messageId);
    public void saveMessage(TalkMessage message);

    public TalkDelivery findDelivery(String messageId, String clientId);
    public List<TalkDelivery> findDeliveriesForClient(String clientId);
    public List<TalkDelivery> findDeliveriesForClientInState(String clientId, String state);
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

}
