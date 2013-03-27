package com.hoccer.talk.server;

import java.util.Hashtable;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;

public class TalkDatabase {
	
	private static Hashtable<String, TalkClient> allClientsById
		= new Hashtable<String, TalkClient>();

    private static Hashtable<String, Vector<TalkDelivery>> allDeliveriesByMessageId
        = new Hashtable<String, Vector<TalkDelivery>>();

    private static Hashtable<String, Vector<TalkDelivery>> allDeliveriesByClientId
        = new Hashtable<String, Vector<TalkDelivery>>();

    private static Hashtable<String, TalkMessage> allMessagesById
        = new Hashtable<String, TalkMessage>();

	public static TalkClient findClient(String clientId) {
        TalkClient result = allClientsById.get(clientId);
        if(result == null) {
		    result = new TalkClient(clientId);
		    allClientsById.put(clientId, result);
        }
		return result;
	}

    public static void saveClient(TalkClient client) {
        allClientsById.put(client.getClientId(), client);
    }

    public static TalkDelivery findDelivery(String messageId, String clientId) {
        Vector<TalkDelivery> deliveries = allDeliveriesByClientId.get(clientId);
        if(deliveries != null) {
            for(TalkDelivery d: deliveries) {
                if(d.getMessageId().equals(messageId)) {
                    return d;
                }
            }
        }
        return null;
    }

    public static TalkMessage findMessage(String messageId) {
        return allMessagesById.get(messageId);
    }

    public static List<TalkDelivery> findDeliveriesForClient(String clientId) {
        return allDeliveriesByClientId.get(clientId);
    }

    public static List<TalkDelivery> findDeliveriesForMessage(String messageId) {
        return allDeliveriesByMessageId.get(messageId);
    }

	public static void saveMessage(TalkMessage m) {
		allMessagesById.put(m.getMessageId(), m);
	}

    public static void saveDelivery(TalkDelivery delivery) {
        String clientId = delivery.getReceiverId();
        String messageId = delivery.getMessageId();

        Vector<TalkDelivery> clientVec = allDeliveriesByClientId.get(clientId);
        if(clientVec == null) {
            clientVec = new Vector<TalkDelivery>();
            allDeliveriesByClientId.put(clientId, clientVec);
        }
        if(!clientVec.contains(delivery)) {
            clientVec.add(delivery);
        }

        Vector<TalkDelivery> messageVec = allDeliveriesByMessageId.get(messageId);
        if(messageVec == null) {
            messageVec = new Vector<TalkDelivery>();
            allDeliveriesByMessageId.put(messageId, messageVec);
        }
        if(!messageVec.contains(delivery)) {
            messageVec.add(delivery);
        }
    }
	
}
