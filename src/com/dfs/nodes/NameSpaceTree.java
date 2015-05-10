package com.dfs.nodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.blocks.Block;
import com.dfs.failure.FSImage;
import com.dfs.utils.Constants;

public class NameSpaceTree {

	public static NamespaceTreeNode root;

	public NameSpaceTree() {
		FSImage image = new FSImage();
		NamespaceTreeNode node = image.loadNameSpace();

		if (node != null)
			root = node;
		else {
			root = new NamespaceTreeNode(FileType.DIR, "");
		}
	}

	/***
	 * Add a node to the Namespace tree. It can be either a directory or File.
	 * 
	 * @param path
	 * @param replication
	 * @param type
	 * @return
	 */
	public boolean addNode(String path, int replication, FileType type) {

		System.out.println(path);
		String[] dirList = path.split("/");
		synchronized (root) {
			return traverseDir(root, dirList, 0, type);
		}
	}

	/**
	 * Perform a depth first search for the given n-ary tree.
	 * 
	 * @param start
	 *            Initially starts at root
	 * @param dirList
	 *            List of nodes
	 * @param level
	 * @param type
	 * @return whether the path is found or not.
	 */
	private boolean traverseDir(NamespaceTreeNode start, String[] dirList,
			int level, FileType type) {
		boolean visited = false;
		if (level == dirList.length - 2) {
			System.out.println("Dir added");
			boolean flag = false;
			for (NamespaceTreeNode node : start.getChildren()) {
				if (node.getInfo().equals(dirList[level + 1])) {
					flag = true;
					break;
				}
			}
			if (!flag) {
				start.getChildren().add(
						new NamespaceTreeNode(type, dirList[level + 1]));
				visited = true;

			}
			return visited;
		}
		for (NamespaceTreeNode node : start.getChildren()) {
			if (node.getInfo().equals(dirList[level + 1])) {
				return traverseDir(node, dirList, ++level, type);
			}
		}
		return visited;
	}

	/****
	 * Traverse the n-ary file structure using DFS.
	 * 
	 * @param start
	 *            starts at root.
	 * @param dirList
	 *            the path list which needs to be traversed
	 * @param level
	 *            current level.
	 * @param type
	 *            whether dir or file ?
	 * @param dataNodeList
	 *            list of datanodes for the file block.
	 * @param offset
	 *            start offset of the block in the file.
	 * @return Block Id.
	 */
	private String traverseFile(NamespaceTreeNode start, String[] dirList,
			int level, FileType type, List<String> dataNodeList, long offset) {

		String blkId = null;
		if (level == dirList.length - 2) {
			boolean fileCheck = false;

			for (NamespaceTreeNode node : start.getChildren()) {
				if (node.getInfo().equals(dirList[dirList.length - 1])) {
					System.out.println("File added");
					fileCheck = true;
					blkId = node.addBlock(dataNodeList, offset);
				}
			}
			if (!fileCheck) {
				System.out.println("File added");
				NamespaceTreeNode node = new NamespaceTreeNode(type,
						dirList[level + 1], dataNodeList);
				start.getChildren().add(node);
				blkId = node.addBlock(dataNodeList, offset);
			}

			return blkId;
		}
		for (NamespaceTreeNode node : start.getChildren()) {
			if (node.getInfo().equals(dirList[level + 1])) {
				return traverseFile(node, dirList, ++level, type, dataNodeList,
						offset);
			}
		}
		return blkId;
	}

	/***
	 * List the files at the given path.
	 * 
	 * @param sourcePath
	 * @return files contained within that directory.
	 */
	public List<String> listFiles(String sourcePath) {

		Queue<NamespaceTreeNode> queue = new LinkedList<>();
		queue.add(root);
		List<String> list = new ArrayList<>();
		String nodes[] = sourcePath.split("/");

		boolean stop = false;

		if (nodes.length == 0) {
			for (NamespaceTreeNode temp : root.getChildren()) {
				list.add(temp.getInfo());
			}
			System.out.println("Listing files");
			return list;
		}

		while (!queue.isEmpty() && !stop) {

			NamespaceTreeNode node = queue.remove();
			if (node.getInfo().equals(nodes[nodes.length - 1])) {
				stop = true;
				for (NamespaceTreeNode temp : node.getChildren()) {
					list.add(temp.getInfo());
				}
				System.out.println("Listing files ");
				return list;
			}
			for (NamespaceTreeNode temp : node.getChildren()) {
				queue.add(temp);
			}

		}

		return null;
	}

	/***
	 * Put files in the tree
	 * 
	 * @param destinationPath
	 *            path in the tree
	 * @param dataNodeList
	 *            list of datanodes the block is assigned
	 * @param offset
	 *            start offset in the file for the block.
	 * @return block Id
	 */
	public String put(String destinationPath, List<String> dataNodeList,
			long offset) {
		synchronized (root) {
			String dirList[] = destinationPath.split("/");
			return traverseFile(root, dirList, 0, FileType.FILE, dataNodeList,
					offset);
		}

	}

	/**
	 * Given the block Id return the Block.
	 * 
	 * @param blockId
	 *            Block Id generated during the put process
	 * @return Actual block with status.
	 */
	public Block getBlock(String blockId) {
		synchronized (root) {
			return root.getBlockMapping(blockId);
		}
	}

	/***
	 * Traverse to get the list of BlockMap.
	 * 
	 * @param start
	 *            Start at the root
	 * @param dirList
	 *            Path to traverse.
	 * @param level
	 *            current level in the tree
	 * @param type
	 *            whether dir or file?
	 * @return blockIds and datanode List.
	 */
	public List<BlocksMap> dfsTraverse(NamespaceTreeNode start,
			String[] dirList, int level, FileType type) {

		if (level == dirList.length - 1) {
			return start.getFileInfo().getBlkMap();
		}
		for (NamespaceTreeNode node : start.getChildren()) {
			if (node.getInfo().equals(dirList[level + 1])) {
				return dfsTraverse(node, dirList, ++level, type);
			}
		}
		return null;

	}

	public static StringBuilder dfsTraverse(String nodeVal,
			NamespaceTreeNode start, String blkId, StringBuilder builder) {

		if (start.getFileType() == FileType.FILE) {

			for (Block blk : start.getFileInfo().getBlocks()) {
				if (blk.getBlockId().equals(blkId)) {
					builder.append(blkId);
					return builder;
				}
			}

		}

		for (NamespaceTreeNode node : start.getChildren()) {
			builder.append(node.getInfo());
			builder.append("/");
			return dfsTraverse(node.getInfo(), node, blkId, builder);
		}
		return builder;

	}

	// public
	/***
	 * Method sends the sorted list of blocks and datanode mappings according to
	 * byte offset.
	 * 
	 * @param sourcePath
	 *            path to traverse
	 * @return sorted list according to byte offset.
	 */
	public List<BlocksMap> getBlockMap(String sourcePath) {
		String[] path = sourcePath.split("/");
		synchronized (root) {
			List<BlocksMap> b = dfsTraverse(root, path, 0, FileType.FILE);
			Collections.sort(b);

			return b;
		}
	}

	public static void main(String[] args) {
		// System.out.println(Constants.NAMENODE_IMAGE_DIR);
		NameSpaceTree tree = new NameSpaceTree();
		System.out.println(tree.addNode("/user", 3, FileType.DIR));
		System.out.println(tree.addNode("/user/kanth", 3, FileType.DIR));

		List<String> dataNodeList = new ArrayList<>();
		dataNodeList.add("1");
		dataNodeList.add("2");
		dataNodeList.add("3");

		System.out.println(tree.put("/user/kanth/file1.txt", dataNodeList, 20));
		System.out.println(tree.put("/user/kanth/file1.txt", dataNodeList, 10));
		System.out.println(tree.put("/user/file2.txt", dataNodeList, 30));
		System.out.println(tree.put("/uss/file.1", dataNodeList, 10));
		System.out.println(tree.listFiles("/user"));

		List<BlocksMap> blkMap = tree.getBlockMap("/user/kanth/file1.txt");
		for (BlocksMap b : blkMap) {
			System.out.println("Block offset: " + b.getBlk().getOffset());
			System.out.println("Block List: " + b.getDatanodeInfo());
			// update(b.getBlk().getBlockId(), "1", "4");
			System.out.println(dfsTraverse("", root, b.getBlk().getBlockId(),
					new StringBuilder().append("/")).toString());
		}

		// ExecutorService service = Executors.newFixedThreadPool(2);
		// service.execute(new FSImage());

	}

	/***
	 * Traverse the Namespace tree to delete and add
	 * 
	 * @param start
	 * @param dirList
	 * @param level
	 * @param ipAddress
	 * @param blkId
	 * @param newIp
	 * @return
	 */
	public static boolean dfsTraverse(NamespaceTreeNode start,
			String ipAddress, String blkId, String newIp) {

		if (start.getFileInfo() != null) {
			for (BlocksMap blkMap : start.getFileInfo().getBlkMap()) {
				if (blkMap.getBlk().getBlockId().equals(blkId)) {
					boolean deleted = blkMap.getDatanodeInfo()
							.remove(ipAddress);
					if (deleted)
						blkMap.getDatanodeInfo().add(newIp);

					System.out.println("BLockMap dfs NodeList: "
							+ blkMap.getDatanodeInfo());
					return true;
				}
			}
		}

		for (NamespaceTreeNode node : start.getChildren()) {
			return dfsTraverse(node, ipAddress, blkId, newIp);
		}
		return false;

	}

	/***
	 * Updates to the Name space tree. Add new Ip address containing the block.
	 * 
	 * @param blkId
	 * @param ipAddress
	 * @param newIpAddress
	 */
	public static void update(String blkId, String ipAddress,
			String newIpAddress) {

		synchronized (root) {
			updateBlockDataMap(blkId, ipAddress, newIpAddress);
			updateDataNodeBlockMap(blkId, ipAddress, newIpAddress);
			dfsTraverse(root, ipAddress, blkId, newIpAddress);
		}
	}

	private static void updateDataNodeBlockMap(String blkId, String ipAddress,
			String newIpAddress) {
		if (NamespaceTreeNode.dataNodeBlockMap.containsKey(ipAddress)) {

			List<String> blkIds = NamespaceTreeNode.dataNodeBlockMap
					.get(ipAddress);
			boolean deleted = blkIds.remove(blkId);
			if (deleted) {
				List<String> newIpBlks = NamespaceTreeNode.dataNodeBlockMap
						.get(newIpAddress);
				if (newIpBlks == null)
					newIpBlks = new ArrayList<>();
				newIpBlks.add(blkId);
				System.out.println("dataNodeBlockMap NodeList: " + newIpBlks);
				NamespaceTreeNode.dataNodeBlockMap.put(blkId, newIpBlks);
			}
		}
	}

	private static void updateBlockDataMap(String blkId, String ipAddress,
			String newIpAddress) {

		if (NamespaceTreeNode.blockDataMap.containsKey(blkId)) {
			List<String> blockToDataNode = NamespaceTreeNode.blockDataMap
					.get(blkId);
			Iterator<String> itr = blockToDataNode.iterator();
			while (itr.hasNext()) {
				String element = itr.next();
				if (element.equals(ipAddress)) {
					itr.remove();
					blockToDataNode.add(newIpAddress);
					NamespaceTreeNode.blockDataMap.put(blkId, blockToDataNode);
					System.out.println(NamespaceTreeNode.blockDataMap
							.get(blkId));
					break;
				}
			}

		}
	}

}
