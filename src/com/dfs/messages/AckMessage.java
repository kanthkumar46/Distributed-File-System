package com.dfs.messages;

import java.io.Serializable;

public class AckMessage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String blkId;
    private String ipAddress;
	
	public AckMessage(String blkId, String ipAddress) {
		this.setBlockId(blkId);
		this.setIpAddress(ipAddress);
	}

	public String getBlockId() {
		return blkId;
	}
	public void setBlockId(String blockId) {
		this.blkId = blockId;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	
}
