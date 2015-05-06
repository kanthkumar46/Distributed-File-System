package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.RequestType;

public class PutNameNodeReplyMessage extends NameNodeReplyMessage{


	private List<String> dataNodeList;
	private String sourcePath;
	private String blkId;
	private static final long serialVersionUID = 1L;
	private String destinationPath;

	public PutNameNodeReplyMessage(String sourcePath,String blkId,List<String> dataNodeList) {
		super(0, RequestType.PUT);
		this.setDataNodeList(dataNodeList);

		this.sourcePath=sourcePath;
		this.blkId = blkId;
		

		this.setSourcePath(sourcePath);
		this.setBlkId(blkId);

		
	}

	public PutNameNodeReplyMessage(String sourcePath2, String success,
			List<String> dataNodeList2, String destinationPath) {
		super(0,RequestType.PUT);
		this.dataNodeList = dataNodeList2;
		this.sourcePath = sourcePath2;
		this.blkId = success;
		this.setDestinationPath(destinationPath);
	}

	public List<String> getDataNodeList() {
		return dataNodeList;
	}

	public void setDataNodeList(List<String> dataNodeList) {
		this.dataNodeList = dataNodeList;
	}

	public String getSourcePath() {
		return sourcePath;
	}

	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getBlkId() {
		return blkId;
	}

	public void setBlkId(String blkId) {
		this.blkId = blkId;
	}

	public String getDestinationPath() {
		return destinationPath;
	}

	public void setDestinationPath(String destinationPath) {
		this.destinationPath = destinationPath;
	}
	
}