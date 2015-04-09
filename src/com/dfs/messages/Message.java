package com.dfs.messages;

public class Message {
	private String ipAddress;
	private int portNum;
	private String fileName;
	private int replication;
	
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

	public String getMessage() {
		return fileName;
	}

	public void setMessage(String fileName) {
		this.fileName = fileName;
	}

	public Message(String ipAddress, int portNum, String fileName,int replication){
		this.ipAddress = ipAddress;
		this.portNum = portNum;
		this.fileName = fileName;
	}

	public int getReplication() {
		return replication;
	}

	public void setReplication(int replication) {
		this.replication = replication;
	}
}
