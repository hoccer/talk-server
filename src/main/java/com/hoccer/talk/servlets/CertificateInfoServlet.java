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

        final P12CertificateChecker p12Verifier = new P12CertificateChecker(config.getApnsCertPath(), config.getApnsCertPassword());

        String result = "{" +
                "\"apns\" : {" +
                "    \"expirationDate\" : \"" + p12Verifier.getCertificateExpiryDate() + "\"," +
                "    \"isExpired\"      : \"" + p12Verifier.isExpired() + "\"" +
                "}}";
        response.getWriter().print(result);
    }
}
