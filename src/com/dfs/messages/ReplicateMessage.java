package com.dfs.messages;

import java.io.Serializable;

import com.dfs.nodes.RequestType;

public class ReplicateMessage implements Serializable{

	private static final long serialVersionUID = 1L;
	String ipAddress;
	int portNum;
	String BlkId;
	RequestType type;
	String path;
	
	public ReplicateMessage(RequestType type, String ipAddress,int portNum,String blkId,String path){
		this.type = type;
		this.ipAddress = ipAddress;
		this.portNum = portNum;
		this.BlkId = blkId;
		this.path = path;
	}
}
