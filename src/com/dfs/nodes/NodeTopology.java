package com.dfs.nodes;

import java.util.Map;

public class NodeTopology implements Comparable{

	private Map<String,Integer> nodeList;
	private int replication;
	
	public NodeTopology (Map<String,Integer> nodeList){
		this.nodeList = nodeList;
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	
	
}
