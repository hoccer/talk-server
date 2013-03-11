package com.hoccer.talk.server;

import java.util.Hashtable;
import java.util.UUID;
import java.util.Vector;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkDelivery;
import com.hoccer.talk.model.TalkMessage;

public class TalkDatabase {
	
	private static Hashtable<String, TalkClient> allClientsById
		= new Hashtable<String, TalkClient>();

    private static Hashtable<String, Vector<TalkDelivery>> allDeliveriesByClientId
        = new Hashtable<String, Vector<TalkDelivery>>();

    private static Hashtable<String, TalkMessage> allMessagesById
        = new Hashtable<String, TalkMessage>();

	public static TalkClient findClient(String clientId) {
		TalkClient result = new TalkClient(clientId);
		allClientsById.put(clientId, result);
		return result;
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

	public static void saveMessage(TalkMessage m) {
		allMessagesById.put(m.getMessageId(), m);
	}

    public static void saveDelivery(TalkDelivery delivery) {
        String clientId = delivery.getReceiverId();
        Vector<TalkDelivery> vec = allDeliveriesByClientId.get(clientId);
        if(vec == null) {
            vec = new Vector<TalkDelivery>();
            allDeliveriesByClientId.put(clientId, vec);
        }
        vec.add(delivery);
    }
	
}
