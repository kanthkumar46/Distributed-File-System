package com.dfs.nodes;

import java.util.List;

public class BlocksMap {

	private String blkId;
	private List<String> datanodeInfo;
	
	public BlocksMap(String blkId, List<String> datanodeInfo) {
		this.blkId = blkId;
		this.datanodeInfo = datanodeInfo;
	}
	
	public String getBlkId() {
		return blkId;
	}
	public void setBlkId(String blkId) {
		this.blkId = blkId;
	}
	public List<String> getDatanodeInfo() {
		return datanodeInfo;
	}
	public void setDatanodeInfo(List<String> datanodeInfo) {
		this.datanodeInfo = datanodeInfo;
	}
}
