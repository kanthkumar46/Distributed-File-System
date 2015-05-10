package com.dfs.failure;

import java.io.Serializable;

import com.dfs.nodes.NamespaceTreeNode;



public class EditLog implements Serializable{
	
	public static final byte OP_MKDIR = 0;
	public static final byte OP_PUT =1;
	
	
	public EditLog(){
		
	}
	
	public void logMkdir (byte opCode, NamespaceTreeNode node){
		
	}
	
	//public void log

}
