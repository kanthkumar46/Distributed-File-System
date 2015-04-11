package com.dfs.nodes;

import java.util.ArrayList;
import java.util.List;

public class NamespaceTreeNode {

	private String info;
	private List<NamespaceTreeNode> children;
	private FileType fileType;
	
	public NamespaceTreeNode(FileType type){
		 children = new ArrayList<>();
		 info=null;
		 this.setFileType(type);
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
	
	
	
}
