package com.hoccer.talk.server;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;

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
    public List<TalkDelivery> findDeliveriesForMessage(String messageId);
    public void saveDelivery(TalkDelivery delivery);

}
