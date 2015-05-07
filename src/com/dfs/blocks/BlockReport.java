package com.dfs.blocks;

import java.io.Serializable;
import java.util.Date;

public class BlockReport implements Serializable{

	private static final long serialVersionUID = 1L;
	private String blkId;
	private Date generationTimeStamp;
	private String ipAddress;
	
	public BlockReport(String blkId,Date gTime,String ipAddress){
		this.setBlkId(blkId);
		this.setGenerationTimeStamp(gTime);
		this.setIpAddress(ipAddress);
	}

	public String getBlkId() {
		return blkId;
	}

	public void setBlkId(String blkId) {
		this.blkId = blkId;
	}

	public Date getGenerationTimeStamp() {
		return generationTimeStamp;
	}

	public void setGenerationTimeStamp(Date generationTimeStamp) {
		this.generationTimeStamp = generationTimeStamp;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
}
