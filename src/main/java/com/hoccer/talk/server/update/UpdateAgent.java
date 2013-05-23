package com.hoccer.talk.server.update;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkGroup;
import com.hoccer.talk.model.TalkGroupMember;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.model.TalkRelationship;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.rpc.TalkRpcConnection;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class UpdateAgent {

    private static final Logger LOG = Logger.getLogger(UpdateAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    private ITalkServerDatabase mDatabase;

    public UpdateAgent(TalkServer server) {
        mExecutor = Executors.newScheduledThreadPool(TalkServerConfiguration.THREADS_UPDATE);
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    public void requestPresenceUpdate(final String clientId) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                // retrieve the current presence of the client
                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                // if we actually have a presence
                if(presence != null) {
                    // determine the connection status of the client
                    boolean isConnected = mServer.isClientConnected(clientId);
                    String connStatus = isConnected ? TalkPresence.CONN_STATUS_ONLINE
                                                    : TalkPresence.CONN_STATUS_OFFLINE;
                    // update the presence with the connection status
                    presence.setConnectionStatus(connStatus);
                    // propagate the presence to all friends
                    performPresenceUpdate(presence);
                }
            }
        });
    }

    private void performPresenceUpdate(TalkPresence presence) {
        // own client id
        String clientId = presence.getClientId();
        // set to collect clients into
        Set<String> clients = new HashSet<String>();
        // collect clients known through relationships
        List<TalkRelationship> relationships = mDatabase.findRelationshipsByOtherClient(clientId);
        for(TalkRelationship relationship: relationships) {
            // if the relation is friendly
            if(relationship.isFriend()) {
                clients.add(relationship.getClientId());
            }
        }
        // collect clients known through groups
        List<TalkGroupMember> ownMembers = mDatabase.findGroupMembersForClient(clientId);
        for(TalkGroupMember ownMember: ownMembers) {
            String groupId = ownMember.getGroupId();
            if(ownMember.isMember()) {
                List<TalkGroupMember> otherMembers = mDatabase.findGroupMembersById(groupId);
                for(TalkGroupMember otherMember: otherMembers) {
                    if(otherMember.isMember()) {
                        clients.add(otherMember.getClientId());
                    }
                }
            }
        }
        // remove self
        clients.remove(clientId);
        // send presence updates
        for(String client: clients) {
            // look for a connection by the other client
            TalkRpcConnection connection = mServer.getClientConnection(client);
            // and if the corresponding client is online
            if(connection != null && connection.isLoggedIn()) {
                // tell the client about the new presence
                connection.getClientRpc().presenceUpdated(presence);
            }
        }
    }

    public void requestRelationshipUpdate(final TalkRelationship relationship) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                TalkRpcConnection clientConnection = mServer.getClientConnection(relationship.getClientId());
                if(clientConnection != null && clientConnection.isLoggedIn()) {
                    clientConnection.getClientRpc().relationshipUpdated(relationship);
                }
            }
        });
    }

    public void requestGroupUpdate(final String groupId) {
        LOG.info("requesting group update " + groupId);
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                TalkGroup updatedGroup = mDatabase.findGroupById(groupId);
                if(updatedGroup != null) {
                    List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                    for(TalkGroupMember member: members) {
                        if(member.isMember()) {
                            TalkRpcConnection connection = mServer.getClientConnection(member.getClientId());
                            if(connection == null || !connection.isConnected()) {
                                continue;
                            }
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
        });
    }

    public void requestGroupMembershipUpdate(final String groupId, final String clientId) {
        LOG.info("requesting group update " + groupId + "/" + clientId);
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                TalkGroupMember updatedMember = mDatabase.findGroupMemberForClient(groupId, clientId);
                if(updatedMember == null) {
                    return;
                }
                List<TalkGroupMember> members = mDatabase.findGroupMembersById(groupId);
                for(TalkGroupMember member: members) {
                    if(member.isMember()) {
                        TalkRpcConnection connection = mServer.getClientConnection(member.getClientId());
                        if(connection == null || !connection.isConnected()) {
                            continue;
                        }
                        ITalkRpcClient rpc = connection.getClientRpc();
                        try {
                            rpc.groupMemberUpdated(updatedMember);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
    }

}
