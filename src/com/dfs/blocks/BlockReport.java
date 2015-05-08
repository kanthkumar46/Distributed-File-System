package com.dfs.blocks;

import java.io.Serializable;
import java.util.List;

public class BlockReport implements Serializable{

	private static final long serialVersionUID = 1L;
	private List<String> blkId;
	private String ipAddress;
	
	
	public BlockReport(List<String> blkId,String ipAddress){
		this.setBlkId(blkId);
		this.setIpAddress(ipAddress);
	}

	public List<String> getBlkId() {
		return blkId;
	}

	public void setBlkId(List<String> blkId) {
		this.blkId = blkId;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
}
