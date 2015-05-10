package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.RequestType;

public class PutNameNodeReplyMessage extends NameNodeReplyMessage{

	private List<String> dataNodeList;
	private String chunkPath;
	private String blkId;
	private static final long serialVersionUID = 1L;


	public PutNameNodeReplyMessage(String chunkPath, String blkId,
			List<String> dataNodeList) {
		super(blkId == null ? -1:0,RequestType.PUT);
		this.setDataNodeList(dataNodeList);
		this.setChunkPath(chunkPath);
		this.setBlkId(blkId);

	}

	public List<String> getDataNodeList() {
		return dataNodeList;
	}

	public void setDataNodeList(List<String> dataNodeList) {
		this.dataNodeList = dataNodeList;
	}

	public String getBlkId() {
		return blkId;
	}

	public void setBlkId(String blkId) {
		this.blkId = blkId;
	}

	public String getChunkPath() {
		return chunkPath;
	}

	public void setChunkPath(String chunkPath) {
		this.chunkPath = chunkPath;
	}
	
}