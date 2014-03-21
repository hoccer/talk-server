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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        String connStatus = isConnected ? TalkPresence.CONN_STATUS_ONLINE
                : TalkPresence.CONN_STATUS_OFFLINE;
        // update the presence with the connection status
        presence.setConnectionStatus(connStatus);
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
                            // XXX only if otherMember != member
                            if (otherMember.isJoined() || otherMember.isInvited()) {
                                String clientId = otherMember.getClientId();
                                LOG.debug("RPUFG: delivering presence of " + clientId);
                                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                                updateConnectionStatus(presence);

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

    public void requestPresenceUpdate(final String clientId) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                // retrieve the current presence of the client
                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                // if we actually have a presence
                if (presence != null) {
                    // update connection status
                    updateConnectionStatus(presence);
                    // propagate the presence to all friends
                    performPresenceUpdate(presence);
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    private void performPresenceUpdate(TalkPresence presence) {
        String tag = "RPU-" + presence.getClientId() + ": ";

        LOG.debug(tag + "commencing");

        // own client id
        String clientId = presence.getClientId();
        // set to collect clients into
        Set<String> clients = new HashSet<String>();
        // collect clients known through relationships
        List<TalkRelationship> relationships = mDatabase.findRelationshipsByOtherClient(clientId);
        for (TalkRelationship relationship : relationships) {
            // if the relation is friendly
            if (relationship.isFriend()) { // XXX what about isBlocked()?
                LOG.debug(tag + "including friend " + relationship.getClientId());
                clients.add(relationship.getClientId());
            }
        }
        // collect clients known through groups
        List<TalkGroupMember> ownMembers = mDatabase.findGroupMembersForClient(clientId);
        for (TalkGroupMember ownMember : ownMembers) {
            String groupId = ownMember.getGroupId();
            if (ownMember.isJoined() || ownMember.isInvited()) {
                LOG.debug(tag + "scanning group " + groupId);
                List<TalkGroupMember> otherMembers = mDatabase.findGroupMembersById(groupId);
                for (TalkGroupMember otherMember : otherMembers) {
                    if (otherMember.isJoined() || ownMember.isInvited()) { // MARK
                        LOG.debug(tag + "including group member " + otherMember.getClientId());
                        clients.add(otherMember.getClientId());
                    } else {
                        LOG.debug(tag + "not including group member " + otherMember.getClientId() + " in state " + otherMember.getState());
                    }
                }
            }
        }
        // remove self
        LOG.debug(tag + "excluding self " + clientId);
        clients.remove(clientId);
        // send presence updates
        for (String client : clients) {
            // look for a connection by the other client
            TalkRpcConnection connection = mServer.getClientConnection(client);
            // and if the corresponding client is online
            if (connection != null && connection.isLoggedIn()) {
                LOG.debug(tag + "client " + client + " is connected");
                try {

                    // Calling Client via RPC
                    // tell the client about the new presence
                    connection.getClientRpc().presenceUpdated(presence);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            } else {
                LOG.debug(tag + "client " + client + " is disconnected");
            }
        }
        LOG.debug(tag + "complete");
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

    public void requestGroupMembershipUpdate(final String groupId, final String clientId) {
        Runnable notificationGenerator = new Runnable() {
            @Override
            public void run() {
                TalkGroupMember updatedMember = mDatabase.findGroupMemberForClient(groupId, clientId);
                if (updatedMember == null) {
                    return;
                }
                List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                for (TalkGroupMember member : members) {
                    if (member.isJoined() || member.isInvited() || member.isGroupRemoved() || member.getClientId().equals(clientId)) {
                        TalkRpcConnection connection = mServer.getClientConnection(member.getClientId());
                        if (connection == null || !connection.isConnected()) {
                            continue;
                        }

                        // Calling Client via RPC
                        ITalkRpcClient rpc = connection.getClientRpc();
                        try {
                            rpc.groupMemberUpdated(updatedMember);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        queueOrExecute(context, notificationGenerator);
    }

    public void setRequestContext() {
        setRequestContext(context);
    }

    public void clearRequestContext() {
        clearRequestContext(context);
    }

}
