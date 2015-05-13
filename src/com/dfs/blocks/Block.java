package com.dfs.blocks;

import java.io.Serializable;

public class Block implements Comparable<Block>,Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String blockId;
	private BlockStatus status;
	private Long offset;
	private int replicateAckCount;
	
	
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

	public Block(String blockId, BlockStatus status,long offset,int replicationCnt){
		this.blockId = blockId;
		this.status = status;
		this.offset = offset;
		this.setReplicateAckCount(replicationCnt);
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

	public int getReplicateAckCount() {
		return replicateAckCount;
	}

	public void setReplicateAckCount(int replicateAckCount) {
		this.replicateAckCount = replicateAckCount;
	}
	
	
}
