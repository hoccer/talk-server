package com.hoccer.talk.server;

import com.hoccer.talk.server.model.TalkMessage;

public interface TalkService {
	
	void identify(String clientId);
	
	void requestDelivery();
	
}
