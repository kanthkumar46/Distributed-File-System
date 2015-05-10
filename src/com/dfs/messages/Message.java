package com.dfs.messages;

import java.io.Serializable;

import com.dfs.nodes.RequestType;
import com.dfs.utils.Constants;

public class Message implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String ipAddress;
	private int portNum;
	private String sourcePath;
	private String destinationPath;
	private String directoryPath;
	private RequestType requestType;
	private int replication;
	private long blockByteOffset;
	private long sourceFileLength;
	
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


	public Message(String ipAddress, int portNum, String sourcePath,String destPath,
			int replication, RequestType type, long byteOffset, long fileLength){
		this.ipAddress = ipAddress;
		this.portNum = portNum;
		this.setSourcePath(sourcePath);
		this.setDestinationPath(destPath);
		if(replication == 0)
			this.replication = Constants.DEFAULT_REPLICATION_FACTOR;
		else
			this.replication = replication;
		this.setRequestType(type);
		this.setBlockByteOffset(byteOffset);
		this.setSourceFileLength(fileLength);
	}

	public Message(String ipAddress, int portNum, String directoryPath, RequestType type){
		this.ipAddress = ipAddress;
		this.portNum = portNum;
		this.setDirectoryPath(directoryPath);
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

	public String getDirectoryPath() {
		return directoryPath;
	}

	public void setDirectoryPath(String directoryPath) {
		this.directoryPath = directoryPath;
	}

	public long getBlockByteOffset() {
		return blockByteOffset;
	}

	public void setBlockByteOffset(long blockByteOffset) {
		this.blockByteOffset = blockByteOffset;
	}

	public long getSourceFileLength() {
		return sourceFileLength;
	}

	public void setSourceFileLength(long sourceFileLength) {
		this.sourceFileLength = sourceFileLength;
	}

}
