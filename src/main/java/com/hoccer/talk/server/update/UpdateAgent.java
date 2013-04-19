package com.hoccer.talk.server.update;

import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkPresence;
import com.hoccer.talk.model.TalkRelationship;
import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnection;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class UpdateAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(UpdateAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    private ITalkServerDatabase mDatabase;

    public UpdateAgent(TalkServer server) {
        mExecutor = Executors.newSingleThreadScheduledExecutor();
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    public void requestPresenceUpdate(final String clientId, final String connStatus) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                TalkPresence presence = mDatabase.findPresenceForClient(clientId);
                if(presence != null) {
                    presence.setConnectionStatus(connStatus);
                    performPresenceUpdate(presence);
                }
            }
        });
    }

    private void performPresenceUpdate(TalkPresence presence) {
        String clientId = presence.getClientId();
        List<TalkRelationship> rels = mDatabase.findRelationshipsByOtherClient(clientId);
        for(TalkRelationship rel: rels) {
            if(rel.getState().equals(TalkRelationship.STATE_FRIEND)) {
                TalkRpcConnection connection = mServer.getClientConnection(rel.getClientId());
                if(connection != null && connection.isLoggedIn()) {
                    connection.getClientRpc().presenceUpdated(presence);
                }
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

}
