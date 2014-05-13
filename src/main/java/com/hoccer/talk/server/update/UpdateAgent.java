package com.hoccer.talk.server.update;

import com.hoccer.talk.model.TalkGroup;
import com.hoccer.talk.model.TalkGroupMember;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.model.TalkRelationship;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.agents.NotificationDeferrer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import com.hoccer.talk.util.MapUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

/**
 * Agent for simple updates (presence, group presence, relationship)
 */
public class UpdateAgent extends NotificationDeferrer {


    private final TalkServer mServer;

    private final ITalkServerDatabase mDatabase;

    private static final ThreadLocal<ArrayList<Runnable>> context = new ThreadLocal<ArrayList<Runnable>>();

    public UpdateAgent(TalkServer server) {
        super(TalkServerConfiguration.THREADS_UPDATE, "update-agent");
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    private void updateConnectionStatus(TalkPresence presence) {
        // determine the connection status of the client
        boolean isConnected = mServer.isClientConnected(presence.getClientId());
         if (presence.getConnectionStatus() == null || isConnected != presence.isConnected()) {
             String connStatus = isConnected ? TalkPresence.CONN_STATUS_ONLINE
                     : TalkPresence.CONN_STATUS_OFFLINE;
            LOG.info("Persisting connection status '" + connStatus + "' for client's presence. ClientId: '" + presence.getClientId() + "'");
            presence.setConnectionStatus(connStatus);
            mDatabase.savePresence(presence);
        }
    }

    public void requestPresenceUpdateForGroup(final String clientId, final String groupId) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                LOG.debug("RPUFG: update " + clientId + " for group " + clientId);
                TalkRpcConnection conn = mServer.getClientConnection(clientId);
                if (conn == null || !conn.isConnected()) {
                    return;
                }
                ITalkRpcClient rpc = conn.getClientRpc();
                try {
                    TalkGroupMember member = mDatabase.findGroupMemberForClient(groupId, clientId);
                    if (member.isInvited() || member.isJoined()) {
                        List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                        for (TalkGroupMember otherMember : members) {
                            // TODO: Check if filtering self(clientId) is necessary
                            // only if otherMember != member
                            if (otherMember.isJoined() || otherMember.isInvited()) {
                                String clientId = otherMember.getClientId();
                                LOG.debug("RPUFG: delivering presence of " + clientId);
                                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                                if (presence.getConnectionStatus() == null) {
                                    updateConnectionStatus(presence);
                                }

                                // Calling Client via RPC
                                rpc.presenceUpdated(presence);
                            } else {
                                LOG.debug("RPUFG: target " + otherMember.getClientId() + " is not invited or joined");
                            }
                        }
                    } else {
                        LOG.debug("RPUFG: not invited or joined in group " + member.getGroupId());
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public void requestPresenceUpdateForClient(final String clientId, final String targetClientId) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                LOG.debug("RPUC: updating " + targetClientId + " with presence of " + clientId);
                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                TalkRpcConnection targetConnection = mServer.getClientConnection(targetClientId);
                if (targetConnection == null || !targetConnection.isConnected()) {
                    LOG.debug("RPUC: target not connected");
                    return;
                }
                updateConnectionStatus(presence);
                try {
                    // Calling Client via RPC
                    targetConnection.getClientRpc().presenceUpdated(presence);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public void requestPresenceUpdate(final String clientId, final Set<String> fields) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                // retrieve the current presence of the client
                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                // if we actually have a presence
                if (presence != null) {
                    if (fields == null || fields.contains(TalkPresence.FIELD_CONNECTION_STATUS)) {
                        // update connection status
                        updateConnectionStatus(presence);
                    }
                    // propagate the presence to all friends
                    performPresenceUpdate(presence, fields);
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    private void performPresenceUpdate(TalkPresence presence, final Set<String> fields) {
        String tag = "RPU-" + presence.getClientId() + ": ";

        LOG.trace(tag + "commencing");

        // own client id
        String selfClientId = presence.getClientId();
        // set to collect clientIds into
        Set<String> clientIds = new HashSet<String>();
        // collect clientIds known through relationships
        List<TalkRelationship> relationships = mDatabase.findRelationshipsByOtherClient(selfClientId);
        for (TalkRelationship relationship : relationships) {
            // if the relation is friendly
            if (relationship.isFriend()) {
                LOG.trace(tag + "including friend " + relationship.getClientId());
                clientIds.add(relationship.getClientId());
            }
            // XXX what about isBlocked()?
        }
        // collect clientIds known through groups
        List<TalkGroupMember> ownMembers = mDatabase.findGroupMembersForClient(selfClientId);
        for (TalkGroupMember ownMember : ownMembers) {
            String groupId = ownMember.getGroupId();
            if (ownMember.isJoined() || ownMember.isInvited()) {
                LOG.trace(tag + "scanning group " + groupId);
                List<TalkGroupMember> otherMembers = mDatabase.findGroupMembersById(groupId);
                for (TalkGroupMember otherMember : otherMembers) {
                    if (otherMember.isJoined() || ownMember.isInvited()) { // MARK
                        LOG.trace(tag + "including group member " + otherMember.getClientId());
                        clientIds.add(otherMember.getClientId());
                    } else {
                        LOG.trace(tag + "not including group member " + otherMember.getClientId() + " in state " + otherMember.getState());
                    }
                }
            }
        }
        // remove self
        LOG.trace(tag + "excluding self " + selfClientId);
        clientIds.remove(selfClientId);

        TalkPresence modifiedPresence = null;
        if (fields != null) {
            modifiedPresence = new TalkPresence();
            modifiedPresence.updateWith(presence, fields);
        }

        // send presence updates
        for (String clientId : clientIds) {
            // look for a connection by the other clientId
            TalkRpcConnection connection = mServer.getClientConnection(clientId);
            // and if the corresponding clientId is online
            if (connection != null && connection.isLoggedIn()) {
                LOG.trace(tag + "clientId " + clientId + " is connected");
                try {
                    // Calling Client via RPC
                    // tell the clientId about the new presence
                    if (fields == null) {
                        connection.getClientRpc().presenceUpdated(presence);
                    } else {
                        connection.getClientRpc().presenceModified(modifiedPresence);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            } else {
                LOG.trace(tag + "clientId " + clientId + " is disconnected");
            }
        }
        LOG.trace(tag + "complete");
    }

    public void requestRelationshipUpdate(final TalkRelationship relationship) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                TalkRpcConnection clientConnection = mServer.getClientConnection(relationship.getClientId());
                if (clientConnection != null && clientConnection.isLoggedIn()) {
                    // Calling Client via RPC
                    clientConnection.getClientRpc().relationshipUpdated(relationship);
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public void requestGroupUpdate(final String groupId, final String clientId) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                TalkGroup updatedGroup = mDatabase.findGroupById(groupId);
                if (updatedGroup != null) {
                    TalkRpcConnection connection = mServer.getClientConnection(clientId);
                    if (connection == null || !connection.isConnected()) {
                        return;
                    }

                    // Calling Client via RPC
                    ITalkRpcClient rpc = connection.getClientRpc();
                    try {
                        rpc.groupUpdated(updatedGroup);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public void requestGroupUpdate(final String groupId) {
        Runnable notification = new Runnable() {
            @Override
            public void run() {
                TalkGroup updatedGroup = mDatabase.findGroupById(groupId);
                if (updatedGroup != null) {
                    List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                    for (TalkGroupMember member : members) {
                        if (member.isJoined() || member.isInvited() || member.isGroupRemoved()) {
                            TalkRpcConnection connection = mServer.getClientConnection(member.getClientId());
                            if (connection == null || !connection.isConnected()) {
                                continue;
                            }

                            // Calling Client via RPC
                            ITalkRpcClient rpc = connection.getClientRpc();
                            try {
                                rpc.groupUpdated(updatedGroup);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        };
        queueOrExecute(context, notification);
    }

    // TODO: optimze update calls based in isNew
    public void requestGroupMembershipUpdate(final String groupId, final String clientId, final boolean isNew) {
        LOG.debug("requestGroupMembershipUpdate for group " + groupId + " client " + clientId);
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                TalkGroupMember updatedMember = mDatabase.findGroupMemberForClient(groupId, clientId);
                if (updatedMember == null) {
                    LOG.debug("requestGroupMembershipUpdate updatedMember is null");
                    return;
                }
                TalkGroupMember foreignMember = new TalkGroupMember();
                foreignMember.foreignUpdateWith(updatedMember);

                List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                LOG.debug("requestGroupMembershipUpdate found "+members.size()+" members");
                boolean someOneWasNotified = false;
                for (TalkGroupMember member : members) {
                    if (member.isJoined() || member.isInvited() || member.isGroupRemoved() || member.getClientId().equals(clientId)) {
                        TalkRpcConnection connection = mServer.getClientConnection(member.getClientId());
                        if (connection == null || !connection.isConnected()) {
                            LOG.debug("requestGroupMembershipUpdate - refrain from updating not connected member client " + member.getClientId());
                            continue;
                        }

                        // Calling Client via RPC
                        ITalkRpcClient rpc = connection.getClientRpc();
                        try {
                            if (member.getClientId().equals(clientId)) {
                                // is own membership
                                rpc.groupMemberUpdated(updatedMember);
                            } else {
                                rpc.groupMemberUpdated(foreignMember);
                            }
                            someOneWasNotified = true;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        LOG.debug("requestGroupMembershipUpdate - not updating client "+ member.getClientId()+", state="+member.getState()+", self="+member.getClientId().equals(clientId));
                    }
                }
                if (someOneWasNotified) {
                    checkAndRequestGroupMemberKeys(groupId);
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    // call once for a new group member, will send out groupMemberUpdated-Notifications to new member with all other group members
    public void requestGroupMembershipUpdatesForNewMember(final String groupId, final String newMemberClientId) {
        LOG.debug("requestGroupMembershipUpdateForNewMember for group " + groupId + " newMemberClientId " + newMemberClientId);
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                TalkGroupMember newMember = mDatabase.findGroupMemberForClient(groupId, newMemberClientId);
                if (newMember == null) {
                    LOG.debug("requestGroupMembershipUpdateForNewMember can't find newMember, is null");
                    return;
                }
                List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                LOG.debug("requestGroupMembershipUpdateForNewMember found "+members.size()+" members");
                TalkRpcConnection connection = mServer.getClientConnection(newMember.getClientId());
                if (connection == null || !connection.isConnected()) {
                    LOG.debug("requestGroupMembershipUpdateForNewMember - new client no longer connected "+ newMember.getClientId());
                    return;
                }
                // Calling Client via RPC
                ITalkRpcClient rpc = connection.getClientRpc();
                for (TalkGroupMember member : members) {
                    // do not send out updates for own membership or dead members
                    if (!member.getClientId().equals(newMemberClientId) && (member.isJoined() || member.isInvited())) {
                        try {
                            member.setEncryptedGroupKey(null);
                            rpc.groupMemberUpdated(member);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        LOG.debug("requestGroupMembershipUpdateForNewMember - not updating with member "+ member.getClientId()+", state="+member.getState());
                    }
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public ArrayList<Pair<TalkGroupMember, Long>> membersSortedByLatency(List<TalkGroupMember> members) {

        Map<TalkGroupMember, Long> membersByLatency = new HashMap<TalkGroupMember, Long>();
        for (TalkGroupMember m : members) {
            TalkRpcConnection connection = mServer.getClientConnection(m.getClientId());
            if (connection != null) {
                // if we dont have a latency, assume something bad
                Long latency = connection.getLastPingLatency();
                if (latency != null) {
                    membersByLatency.put(m, latency + connection.getCurrentPriorityPenalty());
                } else {
                    membersByLatency.put(m, new Long(5000) + connection.getCurrentPriorityPenalty());
                }
            }
        }
        membersByLatency = MapUtil.sortByValue(membersByLatency);

        ArrayList<Pair<TalkGroupMember, Long>> result = new ArrayList<Pair<TalkGroupMember, Long>>();
        for (Map.Entry<TalkGroupMember, Long> entry : membersByLatency.entrySet()) {
            result.add(new ImmutablePair<TalkGroupMember, Long>(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    public void checkAndRequestGroupMemberKeys(final String groupId)  {
        LOG.debug("checkAndRequestGroupMemberKeys for group " + groupId);
        Runnable checker = new Runnable() {
            @Override
            public void run() {
                performCheckAndRequestGroupMemberKeys(groupId);
            }
        };
        queueOrExecute(context, checker);
    }

    private final static Long MAX_ALLOWED_KEY_REQUEST_LATENCY = new Long(10000);

    private void performCheckAndRequestGroupMemberKeys(String groupId)  {
        TalkGroup group = mDatabase.findGroupById(groupId);
        if (group != null && group.exists()) {

            List<TalkGroupMember> members = mDatabase.findGroupMembersByIdWithStates(group.getGroupId(), TalkGroupMember.ACTIVE_STATES);
            if (members.size() > 0) {
                List<TalkGroupMember> outOfDateMembers = new ArrayList<TalkGroupMember>();
                List<TalkGroupMember> keyMasterCandidatesWithCurrentKey = new ArrayList<TalkGroupMember>();
                List<TalkGroupMember> keyMasterCandidatesWithoutCurrentKey = new ArrayList<TalkGroupMember>();

                String sharedKeyId = group.getSharedKeyId();
                String sharedKeyIdSalt = group.getSharedKeyIdSalt();
                if (sharedKeyId == null) {
                    // nobody has supplied a group key yet
                    for (TalkGroupMember m : members) {
                        TalkPresence presence = mDatabase.findPresenceForClient(m.getClientId());
                        if (presence != null && presence.getKeyId() != null) {
                            if (m.isAdmin() && mServer.isClientReady(m.getClientId())) {
                                keyMasterCandidatesWithoutCurrentKey.add(m);
                            }
                            outOfDateMembers.add(m);
                        } else {
                            LOG.error("checkAndRequestGroupMemberKeys: no presence for client " + m.getClientId()+", member of group "+groupId);
                        }
                    }
                } else {
                    // there is a group key
                    for (TalkGroupMember m : members) {
                        TalkPresence presence = mDatabase.findPresenceForClient(m.getClientId());
                        if (presence != null && presence.getKeyId() != null) {
                            if (!sharedKeyId.equals(m.getSharedKeyId()) || !sharedKeyId.equals(presence.getKeyId())) {
                                // member has not the current key
                                outOfDateMembers.add(m);
                                if (m.isAdmin() && mServer.isClientReady(m.getClientId())) {
                                    keyMasterCandidatesWithoutCurrentKey.add(m);
                                }
                            } else {
                                // member has the current key
                                if (m.isAdmin() && mServer.isClientReady(m.getClientId())) {
                                    keyMasterCandidatesWithCurrentKey.add(m);
                                }
                            }
                        }  else {
                            LOG.error("checkAndRequestGroupMemberKeys:(2) no presence for client " + m.getClientId()+", member of group "+groupId);
                        }
                    }
                }
                if (outOfDateMembers.size() > 0) {
                    // we need request some keys
                    if (keyMasterCandidatesWithCurrentKey.size() > 0) {
                        // prefer candidates that already have a key
                        ArrayList<Pair<TalkGroupMember, Long>> candidatesByLatency = membersSortedByLatency(keyMasterCandidatesWithCurrentKey);
                        TalkGroupMember newKeymaster = null;
                        if (candidatesByLatency.get(0).getRight() < MAX_ALLOWED_KEY_REQUEST_LATENCY) {
                            newKeymaster = candidatesByLatency.get(0).getLeft(); // get the lowest latency candidate
                            requestGroupKeys(newKeymaster.getClientId(), group.getGroupId(), sharedKeyId, sharedKeyIdSalt, outOfDateMembers);
                            return;
                        }
                        // fall through to next block if best candidate does not meet MAX_ALLOWED_LATENCY
                    }
                    if (keyMasterCandidatesWithoutCurrentKey.size() > 0) {
                        // nobody with a current key is online, but we have other admins online, so purchase a new group key
                        ArrayList<Pair<TalkGroupMember, Long>> candidatesByLatency = membersSortedByLatency(keyMasterCandidatesWithoutCurrentKey);
                        TalkGroupMember newKeymaster = null;
                        if (candidatesByLatency.get(0).getRight() < MAX_ALLOWED_KEY_REQUEST_LATENCY) {
                            newKeymaster = candidatesByLatency.get(0).getLeft(); // get the lowest latency candidate
                            requestGroupKeys(newKeymaster.getClientId(), group.getGroupId(), null, null, outOfDateMembers);
                            return;
                        }
                        // fall through to next block if best candidate does not meet MAX_ALLOWED_LATENCY
                    }
                    // we have out of date key members, but no suitable candidate for group key generation
                    LOG.warn("performCheckAndRequestGroupMemberKeys:" + outOfDateMembers.size() + " members have no key in group " + groupId + ", but no suitable keymaster available");
                }
            }
        }
    }
    private void requestGroupKeys(String fromClientId, String forGroupId, String forSharedKeyId, String withSharedKeyIdSalt, List<TalkGroupMember> forOutOfDateMembers) {
        ArrayList<String> forClientIdsList = new ArrayList<String>();
        ArrayList<String> withPublicKeyIdsList = new ArrayList<String>();
        for (TalkGroupMember m : forOutOfDateMembers) {
            TalkPresence p = mDatabase.findPresenceForClient(m.getClientId());
            if (p != null && p.getKeyId() != null) {
                withPublicKeyIdsList.add(p.getKeyId());
                forClientIdsList.add(m.getClientId());
                LOG.info("requestGroupKeys, added client='" + m.getClientId() + "', keyId='" + p.getKeyId() + "'");
            } else {
                LOG.error("requestGroupKeys, failed to add client='"+m.getClientId()+"', keyId='" + p.getKeyId()+"'");
            }
        }
        if (withPublicKeyIdsList.size()>0 && withPublicKeyIdsList.size() == forClientIdsList.size()) {
            String [] forClientIds = forClientIdsList.toArray(new String[0]);
            String [] withPublicKeyIds = withPublicKeyIdsList.toArray(new String[0]);
            TalkRpcConnection connection = mServer.getClientConnection(fromClientId);
            if (forSharedKeyId == null) {
                forSharedKeyId = "RENEW";
                withSharedKeyIdSalt = "RENEW";
            }
            ITalkRpcClient rpc = connection.getClientRpc();
            LOG.error("requestGroupKeys, calling getEncryptedGroupKeys("+forGroupId+") on client for " + forClientIds.length+" client(s)");
            String [] newKeyBoxes = rpc.getEncryptedGroupKeys(forGroupId,forSharedKeyId,withSharedKeyIdSalt,forClientIds, withPublicKeyIds);
            LOG.error("requestGroupKeys, call of getEncryptedGroupKeys("+forGroupId+") returned " + newKeyBoxes.length+" items)");
            if (newKeyBoxes != null) {
                boolean responseLengthOk = false;
                if (forSharedKeyId.equals("RENEW")) {
                    // call return array with two additional
                    responseLengthOk = newKeyBoxes.length == forClientIds.length + 2;
                    if (responseLengthOk) {
                        forSharedKeyId = newKeyBoxes[forClientIds.length];
                        withSharedKeyIdSalt = newKeyBoxes[forClientIds.length+1];
                    }
                } else {
                    responseLengthOk = newKeyBoxes.length == forClientIds.length;
                }
                if (responseLengthOk) {
                    connection.resetPriorityPenalty();
                    Date now = new Date();

                    TalkGroup group = mDatabase.findGroupById(forGroupId);
                    group.setSharedKeyId(forSharedKeyId);
                    group.setSharedKeyIdSalt(withSharedKeyIdSalt);
                    group.setLastChanged(now);
                    mDatabase.saveGroup(group);

                    for (int i = 0; i < forClientIds.length;++i) {
                        TalkGroupMember member = mDatabase.findGroupMemberForClient(forGroupId, forClientIds[i]);
                        member.setSharedKeyId(forSharedKeyId);
                        member.setSharedKeyIdSalt(withSharedKeyIdSalt);
                        member.setMemberKeyId(withPublicKeyIds[i]);
                        member.setEncryptedGroupKey(newKeyBoxes[i]);
                        member.setKeySupplier(fromClientId);
                        member.setSharedKeyDate(now); // TODO: remove this and other fields no longer required
                        member.setLastChanged(now);
                        mDatabase.saveGroupMember(member);
                        // now perform a groupMemberUpdate for the affected client so he gets the new key
                        // but only if it is not the member we got the key from
                        if (!member.getClientId().equals(fromClientId)) {
                            // Note: we notify the client directly from this thread, we are the UpdateAgent anyway
                            // and we have all the information fresh and right here
                            TalkRpcConnection memberConnection = mServer.getClientConnection(forClientIds[i]);
                            if (memberConnection != null && memberConnection.isLoggedIn()) {
                                ITalkRpcClient mrpc = memberConnection.getClientRpc();
                                // we send updates only to those members whose key has changed, so we always send the full update
                                try {
                                    mrpc.groupMemberUpdated(member);
                                    mrpc.groupUpdated(group);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                    }
                } else {
                    LOG.error("requestGroupKeys, bad number of keys returned for group " + forGroupId);
                    connection.penalizePriorization(100L); // penalize this client in selection
                    sleepForMillis(1000); // TODO: schedule with delay instead of sleep
                    checkAndRequestGroupMemberKeys(forGroupId); // try again
                }
            }  else {
                LOG.error("requestGroupKeys, no keys returned for group " + forGroupId);
                connection.penalizePriorization(100L); // penalize this client in selection
                sleepForMillis(1000);  // TODO: schedule with delay instead of sleep
                checkAndRequestGroupMemberKeys(forGroupId); // try again
            }
        }  else {
            sleepForMillis(1000); // TODO: schedule with delay instead of sleep
            LOG.error("requestGroupKeys, no presence for any outdated member of group " + forGroupId);
            checkAndRequestGroupMemberKeys(forGroupId); // try again
        }
    }

    private void sleepForMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void setRequestContext() {
        setRequestContext(context);
    }

    public void clearRequestContext() {
        clearRequestContext(context);
    }

}
