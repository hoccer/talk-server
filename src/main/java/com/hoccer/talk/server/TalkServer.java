package com.hoccer.talk.server;

import java.util.Vector;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hoccer.talk.rpc.TalkRpcServer;

import better.jsonrpc.server.JsonRpcServer;

public class TalkServer {
	
	private static final Logger log = Logger.getLogger(TalkServer.class);

	ObjectMapper mMapper;
	
	JsonRpcServer mRpcServer;
	
	Vector<TalkRpcConnection> mConnections = new Vector<TalkRpcConnection>();
	
	public TalkServer() {
		mMapper = new ObjectMapper();
		mRpcServer = new JsonRpcServer(TalkRpcServer.class);
	}
	
	public ObjectMapper getMapper() {
		return mMapper;
	}
	
	public JsonRpcServer getRpcServer() {
		return mRpcServer;
	}
	
	public void connectionOpened(TalkRpcConnection connection) {
		log.info("connection opened");
		mConnections.add(connection);
	}

	public void connectionClosed(TalkRpcConnection connection) {
		log.info("connection closed");
		mConnections.remove(connection);
	}
	
}
