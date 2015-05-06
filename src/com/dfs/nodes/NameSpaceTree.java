package com.dfs.nodes;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.dfs.blocks.Block;

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
		String [] dirList = path.split("/");
		return traverseDir(root,dirList,0,type);
	}
	
	/**
	 * Perform a depth first search for the given n-ary tree.
	 * @param start		Initially starts at root
	 * @param dirList	List of nodes
	 * @param level		
	 * @param type		
	 * @return			whether the path is found or not.
	 */
	private  boolean traverseDir(NamespaceTreeNode start, String [] dirList,int level,FileType type){
		boolean visited = false;
		if(level==dirList.length-2){
			System.out.println("Dir added");
			start.getChildren().add(new NamespaceTreeNode(type,dirList[level+1]));
			visited = true;
			return visited;
		}
		for(NamespaceTreeNode node: start.getChildren()){
			if(node.getInfo().equals(dirList[level+1])){
				return traverseDir(node,dirList,++level,type);
			}
		}
		return visited;
	}
	
	private  String traverseFile(NamespaceTreeNode start, String [] dirList,int level,FileType type,List<String> dataNodeList){
		
		String blkId = null;
		if(level==dirList.length-2){
			boolean fileCheck = false;
			
			for(NamespaceTreeNode node : start.getChildren()){
				if(node.getInfo().equals(dirList[dirList.length-1])){
					System.out.println("File added");
					fileCheck = true;
					blkId = node.addBlock(dataNodeList);
				}
			}
			if(!fileCheck){
				System.out.println("File added");
				NamespaceTreeNode node = new NamespaceTreeNode(type,dirList[level+1],dataNodeList);
				start.getChildren().add(node);
				blkId= node.addBlock(dataNodeList);
			}
			
			return blkId;
		}
		for(NamespaceTreeNode node: start.getChildren()){
			if(node.getInfo().equals(dirList[level+1])){
				return traverseFile(node,dirList,++level,type,dataNodeList);
			}
		}
		return blkId;
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
			System.out.println("Listing files");
			return list;
		}
		
		while(!queue.isEmpty() && !stop){
			
			NamespaceTreeNode node = queue.remove();
			if(node.getInfo().equals(nodes[nodes.length-1])){
				stop = true;
				for(NamespaceTreeNode temp: node.getChildren()){
					list.add(temp.getInfo());
				}
				System.out.println("Listing files ");
				return list;
			}
			for(NamespaceTreeNode temp:node.getChildren()){
				queue.add(temp);
			}
			i++;
		}
		
		return null;
	}

	public String put(String destinationPath,List<String> dataNodeList) {
		String dirList[] = destinationPath.split("/");
		return traverseFile(root,dirList,0,FileType.FILE,dataNodeList);
		
	}	
	
	public void getBlocks(){
		Map<String,Block> blkMap = root.getBlockMapping();
		Set<String> blkIds =blkMap.keySet();
		for(Map.Entry<String, Block> e : blkMap.entrySet()){
			
			System.out.println("Block ID: "+e.getKey());
			System.out.println("Block Status: "+e.getValue().getStatus());
			
		}
	}
	
	public Block getBlock(String blockId) {
		return root.getBlockMapping(blockId);
	}

	public List<BlocksMap> dfsTraverse(NamespaceTreeNode start,String[] dirList,int level,FileType type){
		
		if(level==dirList.length-1){
			return start.getFileInfo().getBlkMap();
		}
		for(NamespaceTreeNode node: start.getChildren()){
			if(node.getInfo().equals(dirList[level+1])){
				return dfsTraverse(node,dirList,++level,type);
			}
		}
		return null;
		
	}
	public List<BlocksMap> getBlockMap(String sourcePath) {
		String [] path = sourcePath.split("/");
		return dfsTraverse(root,path,0,FileType.FILE);
	}
	
	public static void main(String[] args) {
		NameSpaceTree tree = new NameSpaceTree();
		tree.addNode("/user", 3, FileType.DIR);
		List<String> dataNodeList = new ArrayList<>();
		dataNodeList.add("1");
		dataNodeList.add("2");
		dataNodeList.add("3");		
		System.out.println(tree.put("/user/file1.txt", dataNodeList));
		System.out.println(tree.put("/user/file1.txt", dataNodeList));
		System.out.println(tree.put("/user/file2.txt", dataNodeList));
		System.out.println(tree.put("/uss/file.1", dataNodeList));
		System.out.println(tree.listFiles("/user"));
		tree.getBlocks();
		List<BlocksMap> blkMap =tree.getBlockMap("/user/file2.txt");
		for(BlocksMap b:blkMap) {
			System.out.println("Block Id: "+b.getBlkId());
			System.out.println("Block List: "+ b.getDatanodeInfo());
		}
		
	}

	
}
