package com.hoccer.talk.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.hoccer.talk.server.database.JongoDatabase;
import com.hoccer.talk.server.database.OrmliteDatabase;
import com.hoccer.talk.server.rpc.TalkRpcConnectionHandler;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.WebSocketHandler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * Entrypoint to the Talk server
 */
public class TalkServerMain {

    private static final Logger LOG = Logger.getLogger(TalkServerMain.class);

    @Parameter(names = {"-c", "-config"},
            description = "Configuration file to use")
    String config = null;

    private void run() {
        // load configuration
        TalkServerConfiguration config = initializeConfiguration();

        // select and instantiate database backend
        ITalkServerDatabase db = initializeDatabase(config);

        // log about server init
        LOG.info("Initializing talk server");

        // create the talk server
        TalkServer ts = new TalkServer(config, db);

        // log about jetty init
        LOG.info("Initializing jetty");

        // create jetty instance
        Server s = new Server(new InetSocketAddress(config.getListenAddress(), config.getListenPort()));
        // default handler for non-talk http requests
        DefaultHandler fallbackHandler = new DefaultHandler();
        fallbackHandler.setServeIcon(false);

        ServletContextHandler metricsContextHandler = new ServletContextHandler();
        metricsContextHandler.setContextPath("/metrics");
        metricsContextHandler.setInitParameter("show-jvm-metrics", "true");

        metricsContextHandler.addEventListener(new MyMetricsServletContextListener(ts.getMetrics()));
        metricsContextHandler.addServlet(MetricsServlet.class, "/registry");

        // handler for talk websocket connections
        WebSocketHandler clientHandler = new TalkRpcConnectionHandler(ts);
        clientHandler.setHandler(metricsContextHandler);
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
            // load the property file
            LOG.info("Loading configuration from property file " + config);
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

        // return the configuration
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
        JCommander commander = new JCommander(main, args);
        PropertyConfigurator.configure(main.config);
        main.run();
    }

    private static class MyMetricsServletContextListener extends MetricsServlet.ContextListener {
        private MetricRegistry _metricRegistry;

        public MyMetricsServletContextListener(MetricRegistry metricRegistry) {
            _metricRegistry = metricRegistry;
        }

        @Override
        protected MetricRegistry getMetricRegistry() {
            return _metricRegistry;
        }
    }

}
