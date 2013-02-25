package com.hoccer.talk.server.push;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.model.TalkClient;

public class PushAgent {
	
	private static final String GCM_API_KEY = "";
	
	private ExecutorService mExecutor;

	private Sender mSender;
	
	public PushAgent() {
		mExecutor = Executors.newSingleThreadExecutor();
		mSender = new Sender(GCM_API_KEY);
	}
	
	public void submitRequest(final PushRequest request) {
		mExecutor.execute(new Runnable() {
			@Override
			public void run() {
				performRequest(request);
			}
		});
	}

	protected void performRequest(PushRequest request) {
		TalkClient client = request.getClient();
		Message message = new Message.Builder()
			.timeToLive(23)
			.restrictedPackageName(client.getGcmPackage())
			.delayWhileIdle(false)
			.build();
		try {
			mSender.send(message, client.getGcmRegistration(), 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
