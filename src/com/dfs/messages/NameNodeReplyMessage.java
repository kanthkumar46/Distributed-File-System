package com.dfs.messages;

import java.io.Serializable;
import java.util.List;

import com.dfs.nodes.RequestType;

/**
 * This class represents message format, the NameNode replies
 * to Client for read and write requests 
 *
 */

public class NameNodeReplyMessage implements Serializable{
	
	private static final long serialVersionUID = 1L;
		
	private int errorCode;
	private RequestType type;
	
	public NameNodeReplyMessage(int errorCode, RequestType type){
		this.setErrorCode(errorCode);
		this.setType(type);
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public RequestType getType() {
		return type;
	}

	public void setType(RequestType type) {
		this.type = type;
	}
}



