package com.hoccer.talk.servlets;

import com.hoccer.talk.server.TalkServer;
import com.hoccer.talk.server.TalkServerConfiguration;
import com.hoccer.talk.server.cryptoutils.P12CertificateChecker;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class CertificateInfoServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);

        TalkServer server = (TalkServer)getServletContext().getAttribute("server");
        TalkServerConfiguration config = server.getConfiguration();

        final P12CertificateChecker p12ProductionVerifier = new P12CertificateChecker(
                config.getApnsCertProductionPath(),
                config.getApnsCertProductionPassword());
        final P12CertificateChecker p12SandboxVerifier = new P12CertificateChecker(
                config.getApnsCertSandboxPath(),
                config.getApnsCertSandboxPassword());

        String result = "{" +
                "\"apns\" : {" +
                "    \"production\": {" +
                "        \"expirationDate\" : \"" + p12ProductionVerifier.getCertificateExpiryDate() + "\"," +
                "        \"isExpired\"      : \"" + p12ProductionVerifier.isExpired() + "\"" +
                "    }, \"sandbox\": {" +
                "        \"expirationDate\" : \"" + p12SandboxVerifier.getCertificateExpiryDate() + "\"," +
                "        \"isExpired\"      : \"" + p12SandboxVerifier.isExpired() + "\"" +
                "}}}";
        response.getWriter().print(result);
    }
}
