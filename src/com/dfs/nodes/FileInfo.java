package com.dfs.nodes;

import java.util.ArrayList;
import java.util.List;
import com.dfs.blocks.Block;

public class FileInfo {

	private int replication;
	private List<Block> blocks;
	private List<BlocksMap> blkMap;
	
	public FileInfo(Block blk, int replication,List<String> dataNodeList){
		blocks = new ArrayList<>();
		this.setReplication(replication);
		blocks.add(blk);
		blkMap=new ArrayList<>();
		blkMap.add(new BlocksMap(blk.getBlockId(),dataNodeList));
	}
	
	public List<Block> getBlocks() {
		return blocks;
	}

	public void addBlock(Block e,List<String> dataNodeList){
		this.blocks.add(e);
		blkMap.add(new BlocksMap(e.getBlockId(),dataNodeList));
	}

	public void setBlocks(List<Block> blocks) {
		this.blocks = blocks;
	}


	
	public int getReplication() {
		return replication;
	}


	public void setReplication(int replication) {
		this.replication = replication;
	}


	public List<BlocksMap> getBlkMap() {
		return blkMap;
	}


	public void setBlkMap(List<BlocksMap> blkMap) {
		this.blkMap = blkMap;
	}	
	
}
