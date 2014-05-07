package com.hoccer.talk.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.hoccer.scm.GitInfo;
import com.hoccer.talk.server.database.JongoDatabase;
import com.hoccer.talk.server.database.OrmliteDatabase;
import com.hoccer.talk.server.rpc.TalkRpcConnectionHandler;
import com.hoccer.talk.server.cryptoutils.*;
import com.hoccer.talk.servlets.ServerInfoServlet;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.WebSocketHandler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Entrypoint to the Talk server
 */
public class TalkServerMain {

    private static final Logger LOG = Logger.getLogger(TalkServerMain.class);

    @Parameter(names = {"-c", "-config"},
            description = "Configuration file to use")
    private final String config = null;

    private void run() {
        // load configuration
        TalkServerConfiguration config = initializeConfiguration();

        config.report();

        // report APNS expiry
        if (config.isApnsEnabled()) {
            final P12CertificateChecker p12Verifier = new P12CertificateChecker(config.getApnsCertPath(), config.getApnsCertPassword());
            try {
                LOG.info("APNS expiryDate is: " + p12Verifier.getCertificateExpiryDate());
                LOG.info("APNS expiration status: " + p12Verifier.isExpired());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // select and instantiate database backend
        ITalkServerDatabase db = initializeDatabase(config);
        db.reportPing();

        // log about server init
        LOG.info("Initializing talk server");

        // create the talk server
        TalkServer talkServer = new TalkServer(config, db);

        // log about jetty init
        LOG.info("Initializing jetty");

        // create jetty instance
        Server s = new Server(new InetSocketAddress(config.getListenAddress(), config.getListenPort()));

        ServletContextHandler metricsContextHandler = new ServletContextHandler();
        metricsContextHandler.setContextPath("/metrics");
        metricsContextHandler.setInitParameter("show-jvm-metrics", "true");

        metricsContextHandler.addEventListener(new MyMetricsServletContextListener(talkServer.getMetrics()));
        metricsContextHandler.addServlet(MetricsServlet.class, "/registry");

        metricsContextHandler.addEventListener(new MyHealtchecksServletContextListener(talkServer.getHealthCheckRegistry()));
        metricsContextHandler.addServlet(HealthCheckServlet.class, "/health");

        ServletContextHandler serverInfoContextHandler = new ServletContextHandler();
        serverInfoContextHandler.setContextPath("/server");
        serverInfoContextHandler.setAttribute("server", talkServer);
        serverInfoContextHandler.addServlet(ServerInfoServlet.class, "/info");

        // handler for talk websocket connections
        WebSocketHandler clientHandler = new TalkRpcConnectionHandler(talkServer);
        clientHandler.setHandler(metricsContextHandler);
        clientHandler.setHandler(serverInfoContextHandler);
        // set root handler of the server
        s.setHandler(clientHandler);

        // run and stop when interrupted
        try {
            LOG.info("Starting server");
            s.start();
            s.join();
            LOG.info("Server has quit");
        } catch (Exception e) {
            LOG.error("Exception in server", e);
        }
    }

    private TalkServerConfiguration initializeConfiguration() {
        LOG.info("Determining configuration");
        TalkServerConfiguration configuration = new TalkServerConfiguration();

        // configure from file
        if (config != null) {
            Properties properties = null;
            LOG.info("Loading configuration from property file: '" + config + "'");
            try {
                FileInputStream configIn = new FileInputStream(config);
                properties = new Properties();
                properties.load(configIn);
            } catch (FileNotFoundException e) {
                LOG.error("Could not load configuration", e);
            } catch (IOException e) {
                LOG.error("Could not load configuration", e);
            }
            // if we could load it then configure using it
            if (properties != null) {
                configuration.configureFromProperties(properties);
            }
        }

        // also read additional bundled property files
        LOG.info("Loading bundled properties...");
        Properties bundled_properties = new Properties();
        try {
            InputStream bundledConfigIs = TalkServerConfiguration.class.getResourceAsStream("/server.properties");
            bundled_properties.load(bundledConfigIs);
            configuration.setVersion(bundled_properties.getProperty("version"));
        } catch (IOException e) {
            LOG.error("Unable to load bundled configuration", e);
        }

        LOG.info("Loading GIT properties...");
        Properties git_properties = new Properties();
        try {
            InputStream gitConfigIs = TalkServerConfiguration.class.getResourceAsStream("/git.properties");
            if (gitConfigIs != null) {
                git_properties.load(gitConfigIs);
                configuration.setGitInfo(GitInfo.initializeFromProperties(git_properties));
            }
        } catch (IOException e) {
            LOG.error("Unable to load bundled configuration", e);
        }

        return configuration;
    }

    private ITalkServerDatabase initializeDatabase(TalkServerConfiguration config) {
        LOG.info("Determining database");
        String backend = config.getDatabaseBackend();
        if (backend.equals("jongo")) {
            return new JongoDatabase(config);
        }
        if (backend.equals("ormlite")) {
            return new OrmliteDatabase();
        }
        throw new RuntimeException("Unknown database backend: " + backend);
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        TalkServerMain main = new TalkServerMain();
        new JCommander(main, args);
        PropertyConfigurator.configure(main.config);
        main.run();
    }

    private static class MyMetricsServletContextListener extends MetricsServlet.ContextListener {
        private final MetricRegistry _metricRegistry;

        public MyMetricsServletContextListener(MetricRegistry metricRegistry) {
            _metricRegistry = metricRegistry;
        }

        @Override
        protected MetricRegistry getMetricRegistry() {
            return _metricRegistry;
        }
    }

    private static class MyHealtchecksServletContextListener extends HealthCheckServlet.ContextListener {
        private final HealthCheckRegistry _healthCheckRegistry;

        public MyHealtchecksServletContextListener(HealthCheckRegistry healthCheckRegistry) {
            _healthCheckRegistry = healthCheckRegistry;
        }

        @Override
        protected HealthCheckRegistry getHealthCheckRegistry() {
            return _healthCheckRegistry;
        }
    }
}
