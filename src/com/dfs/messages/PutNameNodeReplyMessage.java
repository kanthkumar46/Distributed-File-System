package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.RequestType;

public class PutNameNodeReplyMessage extends NameNodeReplyMessage{


	private List<String> dataNodeList;
	private String sourcePath;
	private String blkId;
	private static final long serialVersionUID = 1L;

	public PutNameNodeReplyMessage(String sourcePath,String blkId,List<String> dataNodeList) {
		super(0, RequestType.PUT);
		this.setDataNodeList(dataNodeList);
		this.sourcePath=sourcePath;
		this.blkId = blkId;
		
	}

	public List<String> getDataNodeList() {
		return dataNodeList;
	}

	public void setDataNodeList(List<String> dataNodeList) {
		this.dataNodeList = dataNodeList;
	}
	
}