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
		System.out.println("Constructor: "+ blk.getBlockId());
		System.out.println("In c:"+datanodeInfo);
		this.blk = blk;
		this.datanodeInfo = datanodeInfo;
	}
	
	public Block getBlk() {
		return blk;
	}
	public void setBlkId(Block blk) {
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
		System.out.println(o);
		System.out.println(this.blk);
		return this.blk.compareTo(o.getBlk());
		
	}
}
