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
        ITalkServerDatabase db = null;
        if(database.equals("jongo")) {
            db = new JongoDatabase();
        }
        if(database.equals("memory")) {
            db = new MemoryDatabase();
        }
        if(db == null) {
            throw new RuntimeException("Invalid database backend: " + database);
        }

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

	public static void main(String[] args) {
        TalkServerMain main = new TalkServerMain();
        JCommander commander = new JCommander(main, args);
        main.run();
	}

}
