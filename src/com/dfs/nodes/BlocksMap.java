package com.dfs.nodes;

import java.io.Serializable;
import java.util.List;

import com.dfs.blocks.Block;

public class BlocksMap implements Comparable<BlocksMap>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Block blk;
	private List<String> datanodeInfo;
	
	public BlocksMap(Block blk, List<String> datanodeInfo) {
		this.setBlk(blk);
		this.setDatanodeInfo(datanodeInfo);
	}
	
	public Block getBlk() {
		return blk;
	}
	public void setBlk(Block blk) {
		this.blk = blk;
	}
	public List<String> getDatanodeInfo() {
		return datanodeInfo;
	}
	public void setDatanodeInfo(List<String> datanodeInfo) {
		this.datanodeInfo = datanodeInfo;
	}

	@Override
	public int compareTo(BlocksMap o) {
		return this.blk.compareTo(o.getBlk());
	}
}
