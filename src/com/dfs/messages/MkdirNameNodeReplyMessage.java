package com.dfs.messages;

import com.dfs.nodes.RequestType;

public class MkdirNameNodeReplyMessage extends NameNodeReplyMessage{

	private static final long serialVersionUID = 1L;

	public MkdirNameNodeReplyMessage(int status) {
		super(status, RequestType.MKDIR);
	}
	
}
