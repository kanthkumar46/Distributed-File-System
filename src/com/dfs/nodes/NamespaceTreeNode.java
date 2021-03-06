package com.dfs.nodes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.swing.RowFilter.Entry;

import com.dfs.blocks.Block;
import com.dfs.blocks.BlockStatus;

public class NamespaceTreeNode implements Serializable{

	private String info;
	private List<NamespaceTreeNode> children;
	private FileType fileType;
	private FileInfo fileInfo;
	private String createdTime;
	private String user;
	private long size;
	private String path;
	
	// blockId to Block Mapping
	static Map<String,Block> blockMap = new HashMap<>();
	
	// dataNode to block mapping
	public static Map<String,List<String>> dataNodeBlockMap = new HashMap<>();
	
	// block to DataNode mapping
	public static Map<String, List<String>> blockDataMap = new HashMap<>();
	
	public NamespaceTreeNode(){
		
	}
	
	public NamespaceTreeNode(FileType type, String info,String ipAddr,long size,String path){
		if (type == FileType.DIR) {
			this.children = new ArrayList<>();
			this.info = info;
			this.setFileType(type);
			this.setFileInfo(null);
			this.setCreatedTime(new Date().toString());
			this.setUser(ipAddr);
			this.setSize(size);
			this.setPath(path);
		} 
	}
	
	public NamespaceTreeNode(FileType type, String info,
			List<String> dataNodeList,String ipAddr,long size,String path) {
		if(type == FileType.FILE) {
			this.children = null;
			this.info = info;
			this.setFileType(type);
			fileInfo = new FileInfo();
			this.setCreatedTime(new Date().toString());
			this.setUser(ipAddr);
			this.setSize(size);
			this.setPath(path);
		}

	}
	
	public Block getBlockMapping(String key){
		return blockMap.get(key);
	}
	
	public Map<String,Block> getBlockMapping(){
		return blockMap;
	}

	
	public String addBlock(List<String> dataNodeList,long offset) {
		String blockId = UUID.randomUUID().toString();
		Block blk = new Block(blockId,
				BlockStatus.PROGRESS,offset,0);
		fileInfo.addBlock(blk, dataNodeList);
		blockMap.put(blockId, blk);

		for(String dataNode: dataNodeList){
			if(dataNodeBlockMap.containsKey(dataNode)){
				
				List<String> blks = dataNodeBlockMap.get(dataNode);
				
				blks.add(blockId);
				dataNodeBlockMap.put(dataNode, blks);
				
			}else{
				ArrayList<String> blks = new ArrayList<>();
				blks.add(blockId);
				
				dataNodeBlockMap.put(dataNode, blks);
			}
		}
		
		blockDataMap.put(blockId, dataNodeList);
		return blockId;
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


	public FileInfo getFileInfo() {
		return fileInfo;
	}

	public void setFileInfo(FileInfo fileInfo) {
		this.fileInfo = fileInfo;
	}

	public String getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(String createdTime) {
		this.createdTime = createdTime;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

}
