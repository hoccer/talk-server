package com.hoccer.talk.servlets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.model.TalkServerInfo;
import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.rpc.TalkRpcConnectionHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.List;

public class ServerInfoServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        TalkServer server = (TalkServer)getServletContext().getAttribute("server");

        TalkServerInfo serverInfo = new TalkServerInfo();
        serverInfo.setServerTime(new Date());
        serverInfo.setVersion(server.getConfiguration().getVersion());
        serverInfo.setCommitId(server.getConfiguration().getGitInfo().commitId);
        List<String> protcolVersions = TalkRpcConnectionHandler.getCurrentProtocolVersions();
        for (String protcolVersion : protcolVersions) {
            serverInfo.addProtocolVersion(protcolVersion);
        }

        ObjectMapper mapper = new ObjectMapper();
        response.getWriter().print(mapper.writeValueAsString(serverInfo));
    }
}
