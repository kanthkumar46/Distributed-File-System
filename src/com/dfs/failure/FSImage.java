package com.dfs.failure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

import com.dfs.nodes.NameSpaceTree;
import com.dfs.nodes.NamespaceTreeNode;
import com.dfs.utils.Constants;

public class FSImage implements Runnable {

	public boolean saveNameSpace() {

		try {

			File file = new File(Constants.NAMENODE_IMAGE_DIR);

			File file2 = new File(Constants.NAMENODE_HIST_DIR);
			
			if (!file.exists() && !file2.exists()){
				file.mkdirs();
				file2.mkdirs();
			}
			else{
				Path source = Paths.get(Constants.NAMENODE_IMAGE_DIR,"lastcheckpoint.tmp");
				Path target = Paths.get(Constants.NAMENODE_HIST_DIR, source.getFileName().toString());
				Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
			}

			FileOutputStream fos = new FileOutputStream(
					Constants.NAMENODE_IMAGE_DIR + File.separator
							+ "lastcheckpoint.tmp");
			ObjectOutputStream oos = new ObjectOutputStream(fos);

			oos.writeObject(NameSpaceTree.root);
			oos.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

	}

	public NamespaceTreeNode loadNameSpace() {
		try {
			FileInputStream fis = new FileInputStream(
					Constants.NAMENODE_IMAGE_DIR + File.separator
							+ "lastcheckpoint.tmp");
			ObjectInputStream ois = new ObjectInputStream(fis);
			NamespaceTreeNode node = (NamespaceTreeNode) ois.readObject();
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
