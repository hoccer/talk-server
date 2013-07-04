package com.hoccer.talk.server.status;

import com.hoccer.talk.server.ITalkServerDatabase;
import com.hoccer.talk.server.ITalkServerStatistics;
import com.hoccer.talk.server.TalkServer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

public class StatusHandler extends AbstractHandler {

    TalkServer mServer;

    AbstractHandler mFallback;

    public StatusHandler(TalkServer server, AbstractHandler fallbackHandler) {
        mServer = server;
        mFallback = fallbackHandler;
    }

    @Override
    public void handle(String target, Request baseRequest,
                       HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        if(target.equals("/status")) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/plain");

            Writer w = new OutputStreamWriter(response.getOutputStream());

            w.write("Hoccer Talk Server -- System Status\n\n");

            w.write(mServer.getNumCurrentConnections() + " connections open, " + mServer.getNumTotalConnections() + " total during uptime\n\n");

            ITalkServerStatistics stats = mServer.getStatistics();
            Map<String, Integer> srvStats = stats.getMap();
            w.write("Server stats (since " + stats.getStartTime() + "):\n");
            for(String key: srvStats.keySet()) {
                w.write(srvStats.get(key) + " " + key + "\n");
            }
            w.write("\n");

            ITalkServerDatabase db = mServer.getDatabase();
            Map<String, Long> dbStats = db.getStatistics();
            if(dbStats == null) {
                w.write("Database does not support statistics\n");
            } else {
                w.write("Database stats:\n");
                for(String key: dbStats.keySet()) {
                    w.write(dbStats.get(key) + " objects of type " + key + "\n");
                }
            }
            w.write("\n");

            w.flush();
            w.close();
        } else {
            mFallback.handle(target, baseRequest, request, response);
        }
    }
}
