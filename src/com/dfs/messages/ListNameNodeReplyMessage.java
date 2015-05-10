package com.dfs.messages;

import java.util.List;

import com.dfs.nodes.RequestType;

public class  ListNameNodeReplyMessage extends NameNodeReplyMessage{

	private static final long serialVersionUID = 1L;
	private List<MetaData> fileList;
	 
	public ListNameNodeReplyMessage(List<MetaData> fileList) {
		super(0,RequestType.LIST);
		this.setFileList(fileList);
	}

	public List<MetaData> getFileList() {
		return fileList;
	}

	public void setFileList(List<MetaData> fileList) {
		this.fileList = fileList;
	}
}

