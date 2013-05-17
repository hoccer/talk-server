package com.hoccer.talk.server.group;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkGroupMember;
import com.hoccer.talk.rpc.ITalkRpcClient;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class GroupAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(GroupAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    private ITalkServerDatabase mDatabase;

    public GroupAgent(TalkServer server) {
        mExecutor = Executors.newScheduledThreadPool(TalkServerConfiguration.THREADS_GROUP);
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    public void requestGroupUpdate(final String groupId, final String clientId) {
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
                    String memberRole = member.getRole();
                    if(memberRole.equals(TalkGroupMember.ROLE_MEMBER)
                            || memberRole.equals(TalkGroupMember.ROLE_ADMIN)) {
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
