package com.dfs.messages;

import java.io.Serializable;
import java.util.List;

import com.dfs.nodes.RequestType;

public class ClientRequestMessage implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String ipAddress;
	private int portNum;
	private String blkId;
	private String sourceFileName;
	private String destinationPath;
	private RequestType requestType;
	private List<String> dataNodeList;
	
	public ClientRequestMessage(String ipAddress, int portNum, String blkId,
			String srcFileName, String destPath, RequestType req, List<String> dnList) {
		this.setIpAddress(ipAddress);
		this.setPortNum(portNum);
		this.setBlkId(blkId);
		this.setSourceFileName(srcFileName);
		this.setDestinationPath(destPath);
		this.setRequestType(req);
		this.setDataNodeList(dnList);
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

	public List<String> getDataNodeList() {
		return dataNodeList;
	}

	public void setDataNodeList(List<String> dataNodeList) {
		this.dataNodeList = dataNodeList;
	}

	public String getBlkId() {
		return blkId;
	}

	public void setBlkId(String blkId) {
		this.blkId = blkId;
	}

	public String getSourceFileName() {
		return sourceFileName;
	}

	public void setSourceFileName(String sourceFileName) {
		this.sourceFileName = sourceFileName;
	}
	
	
}
