package com.dfs.blocks;

public class Block implements Comparable<Block>{

	private String blockId;
	private BlockStatus status;
	private Long offset;
	
	
	/**
	 * @return the offset
	 */
	public Long getOffset() {
		return offset;
	}

	/**
	 * @param offset the offset to set
	 */
	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Block(String blockId, BlockStatus status,long offset){
		this.blockId = blockId;
		this.status = status;
		this.offset = offset;
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

	@Override
	public int compareTo(Block o) {
		
		return this.offset.compareTo(o.offset);
	}
	
	
}
