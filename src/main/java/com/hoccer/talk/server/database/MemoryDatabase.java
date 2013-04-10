package com.hoccer.talk.server.database;

import com.hoccer.talk.model.*;
import com.hoccer.talk.server.ITalkServerDatabase;

import java.util.*;

/**
 * This is a simple in-memory implementation of the Talk database
 *
 * It is intended to be used in testing and for easy development.
 *
 * Note that this code is not, by any means, usable for production.
 *
 * Shortcomings:
 *
 *  * returns non-cloned lists from internal state
 *  * returns null instead of empty lists in several cases
 *  * indices/hashtables are selected ad-hoc
 *
 */
public class MemoryDatabase implements ITalkServerDatabase {

    private Hashtable<String, TalkClient> mClientsById
            = new Hashtable<String, TalkClient>();

    private Hashtable<String, TalkMessage> mMessagesById
            = new Hashtable<String, TalkMessage>();

    private Hashtable<String, Vector<TalkDelivery>> mDeliveriesByClientId
            = new Hashtable<String, Vector<TalkDelivery>>();

    private Hashtable<String, Vector<TalkDelivery>> mDeliveriesByMessageId
            = new Hashtable<String, Vector<TalkDelivery>>();

    private Hashtable<String, TalkToken> mTokensBySecret
            = new Hashtable<String, TalkToken>();

    private Hashtable<String, Vector<TalkRelationship>> mRelationshipsByClientId
            = new Hashtable<String, Vector<TalkRelationship>>();

    private Hashtable<String, TalkPresence> mPresencesByClientId
            = new Hashtable<String, TalkPresence>();


    public MemoryDatabase() {
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

    @Override
    public TalkPresence findPresenceForClient(String clientId) {
        return mPresencesByClientId.get(clientId);
    }

    @Override
    public void savePresence(TalkPresence presence) {
        mPresencesByClientId.put(presence.getClientId(), presence);
    }

    @Override
    public TalkToken findTokenByPurposeAndSecret(String purpose, String secret) {
        TalkToken res = mTokensBySecret.get(secret);
        if(res != null && res.getPurpose().equals(purpose)) {
            return res;
        }
        return null;
    }

    @Override
    public void saveToken(TalkToken token) {
        mTokensBySecret.put(token.getSecret(), token);
    }

    @Override
    public List<TalkRelationship> findRelationships(String client) {
        return mRelationshipsByClientId.get(client);
    }

    @Override
    public TalkRelationship findRelationshipBetween(String client, String otherClient) {
        Vector<TalkRelationship> relationships = mRelationshipsByClientId.get(client);
        if(relationships != null) {
            for(TalkRelationship r: relationships) {
                if(r.getOtherClientId().equals(otherClient)) {
                    return r;
                }
            }
        }
        return null;
    }

    @Override
    public List<TalkRelationship> findRelationshipsChangedAfter(String client, Date lastKnown) {
        List<TalkRelationship> res = new ArrayList<TalkRelationship>();
        Vector<TalkRelationship> relationships = mRelationshipsByClientId.get(client);
        if(relationships != null) {
            for(TalkRelationship r: relationships) {
                if(r.getLastChanged().after(lastKnown)) {
                    res.add(r);
                }
            }
        }
        return res;
    }

    @Override
    public void saveRelationship(TalkRelationship relationship) {
        String clientId = relationship.getClientId();

        Vector<TalkRelationship> clientVec = mRelationshipsByClientId.get(clientId);
        if(clientVec == null) {
            clientVec = new Vector<TalkRelationship>();
            mRelationshipsByClientId.put(clientId, clientVec);
        }
        if(!clientVec.contains(relationship)) {
            clientVec.add(relationship);
        }
    }
}
