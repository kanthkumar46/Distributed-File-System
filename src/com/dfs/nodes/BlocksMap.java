package com.dfs.nodes;

import java.util.List;

import com.dfs.blocks.Block;

public class BlocksMap implements Comparable<BlocksMap>{

	private Block blkId;
	private List<String> datanodeInfo;
	
	public BlocksMap(Block blkId, List<String> datanodeInfo) {
		this.blkId = blkId;
		this.datanodeInfo = datanodeInfo;
	}
	
	public Block getBlkId() {
		return blkId;
	}
	public void setBlkId(Block blkId) {
		this.blkId = blkId;
	}
	public List<String> getDatanodeInfo() {
		return datanodeInfo;
	}
	public void setDatanodeInfo(List<String> datanodeInfo) {
		this.datanodeInfo = datanodeInfo;
	}

	@Override
	public int compareTo(BlocksMap o) {
		return this.blkId.compareTo(o.getBlkId());
		
	}
}
