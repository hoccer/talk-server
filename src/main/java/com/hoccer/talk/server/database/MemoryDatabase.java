package com.hoccer.talk.server.database;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;
import com.hoccer.talk.server.ITalkServerDatabase;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public class MemoryDatabase implements ITalkServerDatabase {

    private Hashtable<String, TalkClient> mClientsById
            = new Hashtable<String, TalkClient>();

    private Hashtable<String, TalkMessage> mMessagesById
            = new Hashtable<String, TalkMessage>();

    private Hashtable<String, Vector<TalkDelivery>> mDeliveriesByClientId
            = new Hashtable<String, Vector<TalkDelivery>>();

    private Hashtable<String, Vector<TalkDelivery>> mDeliveriesByMessageId
            = new Hashtable<String, Vector<TalkDelivery>>();


    public MemoryDatabase() {
        mClientsById = new Hashtable<String, TalkClient>();
        mMessagesById = new Hashtable<String, TalkMessage>();
        mDeliveriesByClientId = new Hashtable<String, Vector<TalkDelivery>>();
        mDeliveriesByMessageId = new Hashtable<String, Vector<TalkDelivery>>();
    }

    @Override
    public TalkClient findClientById(String clientId) {
        TalkClient result = mClientsById.get(clientId);

        // XXX this is a terrible hack
        if(result == null) {
            result = new TalkClient(clientId);
            mClientsById.put(clientId, result);
        }

        return result;
    }

    @Override
    public void saveClient(TalkClient client) {
        mClientsById.put(client.getClientId(), client);
    }

    @Override
    public TalkMessage findMessageById(String messageId) {
        return mMessagesById.get(messageId);
    }

    @Override
    public void saveMessage(TalkMessage message) {
        mMessagesById.put(message.getMessageId(), message);
    }

    @Override
    public TalkDelivery findDelivery(String messageId, String clientId) {
        Vector<TalkDelivery> deliveries = mDeliveriesByClientId.get(clientId);
        if(deliveries != null) {
            for(TalkDelivery d: deliveries) {
                if(d.getMessageId().equals(messageId)) {
                    return d;
                }
            }
        }
        return null;
    }

    @Override
    public List<TalkDelivery> findDeliveriesForClient(String clientId) {
        return mDeliveriesByClientId.get(clientId);
    }

    @Override
    public List<TalkDelivery> findDeliveriesForMessage(String messageId) {
        return mDeliveriesByMessageId.get(messageId);
    }

    @Override
    public void saveDelivery(TalkDelivery delivery) {
        String clientId = delivery.getReceiverId();
        String messageId = delivery.getMessageId();

        Vector<TalkDelivery> clientVec = mDeliveriesByClientId.get(clientId);
        if(clientVec == null) {
            clientVec = new Vector<TalkDelivery>();
            mDeliveriesByClientId.put(clientId, clientVec);
        }
        if(!clientVec.contains(delivery)) {
            clientVec.add(delivery);
        }

        Vector<TalkDelivery> messageVec = mDeliveriesByMessageId.get(messageId);
        if(messageVec == null) {
            messageVec = new Vector<TalkDelivery>();
            mDeliveriesByMessageId.put(messageId, messageVec);
        }
        if(!messageVec.contains(delivery)) {
            messageVec.add(delivery);
        }
    }

}
