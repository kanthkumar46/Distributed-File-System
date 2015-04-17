package com.dfs.nodes;

public class NameSpaceTree {

	private NamespaceTreeNode root;
	
	public NameSpaceTree() {
		root = new NamespaceTreeNode(FileType.DIR,"/");
	}
	
	public void addNode(String path,int replication,FileType type){
		String [] dirList = path.split("\\");
		traverse(root,dirList,0,type);
	}
	
	private boolean traverse(NamespaceTreeNode start, String [] dirList,int level,FileType type){
		boolean visited = false;
		if(level==dirList.length-2){
			start.getChildren().add(new NamespaceTreeNode(type,dirList[level+1]));
			visited = true;
			return visited;
		}
		for(NamespaceTreeNode node: start.getChildren()){
			if(node.getInfo().equals(dirList[level])){
				return traverse(node,dirList,level++,type);
			}
		}
		return visited;
	}
	
	
}
