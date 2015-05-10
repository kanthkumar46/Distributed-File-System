package com.dfs.failure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.dfs.nodes.NameSpaceTree;
import com.dfs.nodes.NamespaceTreeNode;
import com.dfs.utils.Constants;

public class FSImage implements Runnable{

	public boolean saveNameSpace() {

		try {
			
			File file = new File(Constants.NAMENODE_IMAGE_DIR);
			if(!file.exists())
				file.mkdirs();
			FileOutputStream fos = new FileOutputStream(Constants.NAMENODE_IMAGE_DIR+File.separator+"lastcheckpoint.tmp");
			ObjectOutputStream oos = new ObjectOutputStream(fos);
			
			oos.writeObject(NameSpaceTree.root);
			oos.close();
			return true;
		} catch (IOException e) {
			
			return false;
		}
		
	}
	
	public NamespaceTreeNode loadNameSpace(){
		try {
			FileInputStream fis = new FileInputStream(Constants.NAMENODE_IMAGE_DIR+File.separator+"lastcheckpoint.tmp");
			ObjectInputStream ois = new ObjectInputStream(fis);
			NamespaceTreeNode node = (NamespaceTreeNode)ois.readObject();
			return node;
		} catch (IOException | ClassNotFoundException e) {
			return null;
			
		}
		
	}

	@Override
	public void run() {
		saveNameSpace();
		
	}
}
