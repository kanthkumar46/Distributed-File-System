package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.BlocksMap;
import com.dfs.nodes.RequestType;

public class GetNameNodeReplyMessage extends NameNodeReplyMessage {


	private static final long serialVersionUID = 1L;
	private List<BlocksMap> blockMap;
	private String sourcePath;
	
	public GetNameNodeReplyMessage(List<BlocksMap> blkMap) {
		super(blkMap == null ? -1:0, RequestType.PUT);
		this.setBlockMap(blkMap);
		// TODO Auto-generated constructor stub
	}

	public List<BlocksMap> getBlockMap() {
		return blockMap;
	}

	public void setBlockMap(List<BlocksMap> blockMap) {
		this.blockMap = blockMap;
	}

}
