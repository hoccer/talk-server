package com.hoccer.talk.server;

import java.util.Hashtable;
import java.util.UUID;

import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.model.TalkMessage;

public class TalkDatabase {
	
	private static Hashtable<String, TalkClient> allClientsById
		= new Hashtable<String, TalkClient>();
	
	public static TalkClient findClient(String clientId) {
		TalkClient result = new TalkClient(clientId);
		allClientsById.put(clientId, result);
		return result;
	}
	
	
	private static Hashtable<String, TalkMessage> allMessagesById
		= new Hashtable<String, TalkMessage>();
	
	public static TalkMessage newMessage() {
		String id = UUID.randomUUID().toString();
		TalkMessage result = new TalkMessage();
		allMessagesById.put(id, result);
		return result;
	}
	
	public static TalkMessage findMessage(String messageId) {
		return allMessagesById.get(messageId);
	}
	
}
