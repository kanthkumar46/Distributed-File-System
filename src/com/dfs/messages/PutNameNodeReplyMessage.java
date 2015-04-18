package com.dfs.messages;

import com.dfs.nodes.RequestType;

public class PutNameNodeReplyMessage extends NameNodeReplyMessage{

	public PutNameNodeReplyMessage(int errorCode, RequestType type) {
		super(0, RequestType.PUT);
		
	}
	
}