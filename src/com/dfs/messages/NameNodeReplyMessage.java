package com.dfs.messages;

import java.util.List;

/**
 * This class represents message format, the NameNode replies
 * to Client for read and write requests 
 *
 */

public class NameNodeReplyMessage {
	private List<String> dataNodes;
	private String blockId;
	
	
	public NameNodeReplyMessage(){
		
	}
}
