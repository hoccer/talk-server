package com.hoccer.talk.server.push;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.android.gcm.server.Message;
import com.google.android.gcm.server.Sender;
import com.hoccer.talk.logging.HoccerLoggers;
import com.hoccer.talk.model.TalkClient;
import com.hoccer.talk.server.TalkServerConfiguration;

public class PushAgent {

    private static final Logger LOG = HoccerLoggers.getLogger(PushAgent.class);

	private ExecutorService mExecutor;

	private Sender mSender;
	
	public PushAgent() {
		mExecutor = Executors.newSingleThreadExecutor();
		mSender = new Sender(TalkServerConfiguration.GCM_API_KEY);
	}
	
	public void submitRequest(final PushRequest request) {
        LOG.info("submitted request for " + request.getClient().getClientId());
		mExecutor.execute(new Runnable() {
			@Override
			public void run() {
				performRequest(request);
			}
		});
	}

	private void performRequest(PushRequest request) {
        LOG.info("performing push for " + request.getClient().getClientId());
		TalkClient client = request.getClient();
		Message message = new Message.Builder()
			.timeToLive(23)
			.restrictedPackageName(client.getGcmPackage())
			.delayWhileIdle(false)
            .addData("test-field", "test-data")
			.build();
        LOG.info("message: " + message.toString());
		try {
			mSender.send(message, client.getGcmRegistration(), 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
