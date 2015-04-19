package com.dfs.messages;

import com.dfs.nodes.RequestType;

public class PutNameNodeReplyMessage extends NameNodeReplyMessage{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PutNameNodeReplyMessage(int errorCode, RequestType type) {
		super(0, RequestType.PUT);
		
	}
	
}