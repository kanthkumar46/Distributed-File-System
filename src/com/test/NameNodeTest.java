package com.test;

import com.dfs.nodes.FileType;
import com.dfs.nodes.NameSpaceTree;

public class NameNodeTest {
	
	public static void addTest(){
		NameSpaceTree tree = new NameSpaceTree();
		tree.addNode("/user", 0, FileType.DIR);
		tree.addNode("/user/kanth", 0, FileType.DIR);
		tree.listFiles("/user/");
	}
	
	public static void main(String[] args) {
		addTest();
	}

}
