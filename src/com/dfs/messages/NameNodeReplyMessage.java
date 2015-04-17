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
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<String> dataNodes;
	private String blockId;
	private int errorCode;
	private RequestType type;
	
	public NameNodeReplyMessage(List<String> dataNodes, String blkID,int errorCode,RequestType type){
		this.dataNodes = dataNodes;
		this.blockId = blkID;
		this.errorCode = errorCode;
		this.type = type;
	}
}
