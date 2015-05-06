package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.RequestType;

public class  ListNameNodeReplyMessage extends NameNodeReplyMessage{

	private static final long serialVersionUID = 1L;
	private List<String> fileList;
	 
	public ListNameNodeReplyMessage(List<String> fileList) {
		super(0,RequestType.LIST);
		this.setFileList(fileList);
	}

	public List<String> getFileList() {
		return fileList;
	}

	public void setFileList(List<String> fileList) {
		this.fileList = fileList;
	}
}

