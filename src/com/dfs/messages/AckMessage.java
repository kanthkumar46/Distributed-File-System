package com.dfs.messages;

import java.io.Serializable;

public class AckMessage implements Serializable{
	private String blkId;
	
	private String ipAddress;
	
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
