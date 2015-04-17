package com.dfs.messages;

import com.dfs.nodes.RequestType;

public class Message {
	
	private String ipAddress;
	private int portNum;
	private String sourcePath;
	private String destinationPath;
	private RequestType requestType;
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


	public Message(String ipAddress, int portNum, String sourcePath,String destPath,int replication, RequestType type){
		this.ipAddress = ipAddress;
		this.portNum = portNum;
		this.setSourcePath(sourcePath);
		this.setDestinationPath(destPath);
		this.replication = replication;
		this.setRequestType(type);
	}

	public int getReplication() {
		return replication;
	}

	public void setReplication(int replication) {
		this.replication = replication;
	}


	public String getSourcePath() {
		return sourcePath;
	}


	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}

	public String getDestinationPath() {
		return destinationPath;
	}


	public void setDestinationPath(String destinationPath) {
		this.destinationPath = destinationPath;
	}

	public RequestType getRequestType() {
		return requestType;
	}

	public void setRequestType(RequestType requestType) {
		this.requestType = requestType;
	}
}
