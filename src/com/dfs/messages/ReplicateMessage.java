package com.dfs.messages;

import java.io.Serializable;

import com.dfs.nodes.RequestType;

public class ReplicateMessage implements Serializable{

	private static final long serialVersionUID = 1L;

	private String ipAddress;
	private int portNum;
	private String BlkId;
	private RequestType type;
	private String blkPath;
	
	public ReplicateMessage(RequestType type, String ipAddress,int portNum,String blkId,String path){
		this.setType(type);
		this.setIpAddress(ipAddress);
		this.setPortNum(portNum);
		this.setBlkId(blkId);
		this.setBlkPath(path);
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {

		this.ipAddress = ipAddress;
	}

	public int getPortNum() {
		return portNum;
	}

	public void setPortNum(int portNum) {
		this.portNum = portNum;
	}

	public String getBlkId() {
		return BlkId;
	}

	public void setBlkId(String blkId) {
		this.BlkId = blkId;
	}

	public RequestType getType() {
		return type;
	}

	public void setType(RequestType type) {
		this.type = type;
	}

	public String getBlkPath() {
		return blkPath;
	}

	public void setBlkPath(String blkPath) {
		this.blkPath = blkPath;
	}
}
