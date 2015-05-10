package com.dfs.messages;

import java.io.Serializable;

import com.dfs.nodes.FileType;

public class MetaData implements Serializable{

	private static final long serialVersionUID = 1L;
	private String path;
	private String createdTime;
	private String createdUser;
	private FileType type;
	
	public MetaData(String path, String createdTime,String createdUser,FileType type){
		this.path = path;
		this.createdTime = createdTime;
		this.createdUser = createdUser;
		this.type = type;
		
		
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

}
