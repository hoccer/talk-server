package com.hoccer.talk.server;

import java.net.InetSocketAddress;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.hoccer.talk.server.database.JongoDatabase;
import com.hoccer.talk.server.database.MemoryDatabase;
import com.hoccer.talk.server.rpc.TalkRpcConnectionHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.websocket.WebSocketHandler;

public class TalkServerMain {

    @Parameter(names={"-l", "-listen"},
               description = "Address/host to listen on")
    String listen = "0.0.0.0";

    @Parameter(names={"-p", "-port"},
               description = "Port to listen on")
    int port = 8080;

    @Parameter(names={"-d", "-database"},
               description = "Database backend to use (memory or jongo)")
    String database = "jongo";

    private void run() {
        // select and instantiate database backend
        ITalkServerDatabase db = initializeDatabase();

        // create the talk server
        TalkServer ts = new TalkServer(db);

        // create jetty instance
        Server s = new Server(new InetSocketAddress(listen, port));

        // default handler for non-talk http requests
        DefaultHandler fallbackHandler = new DefaultHandler();
        fallbackHandler.setServeIcon(false);

        // handler for talk websocket connections
        WebSocketHandler clientHandler = new TalkRpcConnectionHandler(ts);
        clientHandler.setHandler(fallbackHandler);

        // set root handler of the server
        s.setHandler(clientHandler);

        // run and stop when interrupted
        try {
            s.start();
            s.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ITalkServerDatabase initializeDatabase() {
        if(database.equals("jongo")) {
            return new JongoDatabase();
        }
        if(database.equals("memory")) {
            return new MemoryDatabase();
        }
        throw new RuntimeException("Unknown database backend: " + database);
    }

	public static void main(String[] args) {
        TalkServerMain main = new TalkServerMain();
        JCommander commander = new JCommander(main, args);
        main.run();
	}

}
