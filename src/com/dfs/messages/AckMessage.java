package com.dfs.messages;

public class AckMessage {
	private String blkId;
	private String destinationPath;
	private String ipAddress;
	
	public String getBlcokId() {
		return blkId;
	}
	public void setBlcokId(String blockId) {
		this.blkId = blockId;
	}
	public String getIpAddress() {
		return ipAddress;
	}
	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getDestinationPath() {
		return destinationPath;
	}
	public void setDestinationPath(String destinationPath) {
		this.destinationPath = destinationPath;
	}
	
}
