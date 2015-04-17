package com.dfs.nodes;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class NameSpaceTree {

	private NamespaceTreeNode root;
	
	public NameSpaceTree() {
		root = new NamespaceTreeNode(FileType.DIR,"");
	}
	
	/***
	 * Add a node to the Namespace tree. It can be either a directory or File.
	 * @param path
	 * @param replication
	 * @param type
	 * @return
	 */
	public boolean addNode(String path,int replication,FileType type){
		String [] dirList = path.split("\\");
		return traverse(root,dirList,0,type);
	}
	
	/**
	 * Perform a depth first search for the given n-ary tree.
	 * @param start		Initially starts at root
	 * @param dirList	List of nodes
	 * @param level		
	 * @param type		
	 * @return			whether the path is found or not.
	 */
	private  boolean traverse(NamespaceTreeNode start, String [] dirList,int level,FileType type){
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

	/***
	 * List the files at the given path.
	 * @param sourcePath
	 * @return	files contained within that directory.
	 */
	public List<String> listFiles(String sourcePath) {
		
		Queue<NamespaceTreeNode> queue = new LinkedList<>();
		queue.add(root);
		List<String> list= new ArrayList<>();
		String nodes [] = sourcePath.split("/");
		int i=0;
		boolean stop= false;
		
		while(!queue.isEmpty() && !stop){
			i++;
			NamespaceTreeNode node = queue.remove();
			if(node.getInfo().equals(nodes[i])){
				stop = true;
				for(NamespaceTreeNode temp: node.getChildren()){
					list.add(temp.getInfo());
				}
				return list;
			}
			for(NamespaceTreeNode temp:node.getChildren()){
				queue.add(temp);
			}
		}
		
		return null;
	}	
}
