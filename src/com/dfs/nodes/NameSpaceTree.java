package com.dfs.nodes;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class NameSpaceTree {

	private NamespaceTreeNode root;
	
	public NameSpaceTree() {
		root = new NamespaceTreeNode(FileType.DIR,"",null);
	}
	
	/***
	 * Add a node to the Namespace tree. It can be either a directory or File.
	 * @param path
	 * @param replication
	 * @param type
	 * @return
	 */
	public boolean addNode(String path,int replication,FileType type){
		String [] dirList = path.split("/");
		return traverse(root,dirList,0,type,null);
	}
	
	/**
	 * Perform a depth first search for the given n-ary tree.
	 * @param start		Initially starts at root
	 * @param dirList	List of nodes
	 * @param level		
	 * @param type		
	 * @return			whether the path is found or not.
	 */
	private  boolean traverse(NamespaceTreeNode start, String [] dirList,int level,FileType type,List<String> dataNodeList){
		boolean visited = false;
		if(level==dirList.length-2){
			if(type == FileType.DIR)
				start.getChildren().add(new NamespaceTreeNode(type,dirList[level+1],null));
			else
				start.getChildren().add(new NamespaceTreeNode(type,dirList[level+1],dataNodeList));
			visited = true;
			return visited;
		}
		for(NamespaceTreeNode node: start.getChildren()){
			if(node.getInfo().equals(dirList[level+1])){
				return traverse(node,dirList,++level,type,dataNodeList);
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
		
		if(nodes.length==0){
			for(NamespaceTreeNode temp: root.getChildren()){
				list.add(temp.getInfo());
			}
			return list;
		}
		
		while(!queue.isEmpty() && !stop){
			
			NamespaceTreeNode node = queue.remove();
			if(node.getInfo().equals(nodes[nodes.length-1])){
				stop = true;
				for(NamespaceTreeNode temp: node.getChildren()){
					list.add(temp.getInfo());
				}
				return list;
			}
			for(NamespaceTreeNode temp:node.getChildren()){
				queue.add(temp);
			}
			i++;
		}
		
		return null;
	}

	public boolean put(String destinationPath,List<String> dataNodeList) {
		String dirList[] = destinationPath.split("/");
		return traverse(root,dirList,0,FileType.FILE,dataNodeList);
		
	}	
	
	public static void main(String[] args) {
		NameSpaceTree tree = new NameSpaceTree();
		tree.addNode("/user", 3, FileType.DIR);
		List<String> dataNodeList = new ArrayList<>();
		dataNodeList.add("1");
		dataNodeList.add("2");
		dataNodeList.add("3");		
		tree.put("/user/file1.txt", dataNodeList);
	}
}
