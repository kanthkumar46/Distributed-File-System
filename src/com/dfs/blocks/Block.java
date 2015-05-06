package com.dfs.blocks;

public class Block {

	private String blockId;
	private BlockStatus status;
	
	public Block(String blockId, BlockStatus status){
		this.blockId = blockId;
		this.status = status;
	}

	public String getBlockId() {
		return blockId;
	}

	public void setBlockId(String blockId) {
		this.blockId = blockId;
	}

	public BlockStatus getStatus() {
		return status;
	}

	public void setStatus(BlockStatus status) {
		this.status = status;
	}
	
	
}
