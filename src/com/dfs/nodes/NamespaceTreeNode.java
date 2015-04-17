package com.dfs.nodes;

import java.util.ArrayList;
import java.util.List;

public class NamespaceTreeNode {

	private String info;
	private List<NamespaceTreeNode> children;
	private FileType fileType;
	private FileInfo fileInfo;
	
	public NamespaceTreeNode(FileType type, String info){
		if(type==FileType.DIR){
			this.children = new ArrayList<>();
			this.info=info;
			this.setFileType(type);
			this.setFileInfo(null);
		}
		else{
			this.children = null;
			this.info=info;
			this.setFileType(type);
			this.setFileInfo(new FileInfo());
		}
		
	}


	public String getInfo() {
		return info;
	}


	public void setInfo(String info) {
		this.info = info;
	}

	
	public List<NamespaceTreeNode> getChildren() {
		return children;
	}


	public void setChildren(List<NamespaceTreeNode> children) {
		this.children = children;
	}


	public FileType getFileType() {
		return fileType;
	}


	public void setFileType(FileType fileType) {
		this.fileType = fileType;
	}


	/**
	 * @return the fileInfo
	 */
	public FileInfo getFileInfo() {
		return fileInfo;
	}


	/**
	 * @param fileInfo the fileInfo to set
	 */
	public void setFileInfo(FileInfo fileInfo) {
		this.fileInfo = fileInfo;
	}
	
	
	
}
