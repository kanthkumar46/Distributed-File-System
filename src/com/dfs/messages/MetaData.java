package com.dfs.messages;

import java.io.Serializable;

import com.dfs.nodes.FileType;

public class MetaData implements Serializable{

	private static final long serialVersionUID = 1L;
	private String path;
	private String createdTime;
	private String createdUser;
	private FileType type;
	private long size;
	
	public MetaData(String path, String createdTime,String createdUser,FileType type,long size){
		this.path = path;
		this.createdTime = createdTime;
		this.createdUser = createdUser;
		this.setType(type);
		this.setSize(size);
	}

	public String getCreatedUser() {
		return createdUser;
	}

	public void setCreatedUser(String createdUser) {
		this.createdUser = createdUser;
	}

	public String getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(String createdTime) {
		this.createdTime = createdTime;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public FileType getType() {
		return type;
	}

	public void setType(FileType type) {
		this.type = type;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

}
