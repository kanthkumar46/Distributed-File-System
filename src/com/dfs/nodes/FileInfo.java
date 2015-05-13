package com.dfs.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.dfs.blocks.Block;
import com.dfs.utils.Constants;

public class FileInfo implements Serializable{

	private int replication;
	private List<Block> blocks;
	private List<BlocksMap> blkMap;
	
	public FileInfo(){
		this.replication = Constants.DEFAULT_REPLICATION;
		this.blocks = new ArrayList<>();
		this.blkMap= new ArrayList<>();
	}
	/*public FileInfo(Block blk, int replication,List<String> dataNodeList){
		blocks = new ArrayList<>();
		this.setReplication(replication);
		blocks.add(blk);
		//blkMap=new ArrayList<>();
		blkMap.add(new BlocksMap(blk,dataNodeList));
	}*/
	
	public List<Block> getBlocks() {
		return blocks;
	}

	public void addBlock(Block e,List<String> dataNodeList){
		blocks.add(e);
		blkMap.add(new BlocksMap(e,dataNodeList));
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
