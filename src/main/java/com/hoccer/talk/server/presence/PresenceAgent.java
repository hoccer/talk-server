package com.hoccer.talk.server.presence;

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

public class PresenceAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(PresenceAgent.class);

    private ScheduledExecutorService mExecutor;

    private TalkServer mServer;

    private ITalkServerDatabase mDatabase;

    public PresenceAgent(TalkServer server) {
        mExecutor = Executors.newSingleThreadScheduledExecutor();
        mServer = server;
        mDatabase = mServer.getDatabase();
    }

    public void requestPresenceUpdate(final TalkPresence presence) {
        mExecutor.execute(new Runnable() {
            @Override
            public void run() {
                performPresenceUpdate(presence);
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

}
