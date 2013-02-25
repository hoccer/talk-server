package com.hoccer.talk.server.push;

import com.hoccer.talk.model.TalkClient;

public class PushRequest {

	TalkClient mClient;

	public PushRequest(TalkClient client) {
		mClient = client;
	}
	
	public TalkClient getClient() {
		return mClient;
	}
	
}
