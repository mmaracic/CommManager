package com.wispear.comm;

import com.google.protobuf.GeneratedMessage;

public interface MessageHandler {
	/**
	 * Handle the received message
	 * @param message - The received message
	 * @param peer_id - Relevant if this is a request message by another service. Otherwise null
	 * @param request_id - Relevant if this is request message by another service. Otherwise -1
	 */
	public void handleMessage(GeneratedMessage message, byte[] peer_id, int request_id);
}
