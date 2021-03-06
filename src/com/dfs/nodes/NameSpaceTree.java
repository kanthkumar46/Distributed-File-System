package com.dfs.nodes;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.blocks.Block;
import com.dfs.blocks.BlockStatus;
import com.dfs.failure.FSImage;
import com.dfs.messages.MetaData;
import com.dfs.messages.ReplicateMessage;
import com.dfs.utils.Constants;

public class NameSpaceTree {

	public static NamespaceTreeNode root;

	public NameSpaceTree() {
		FSImage image = new FSImage();
		NamespaceTreeNode node = image.loadNameSpace();

		if (node != null)
			root = node;
		else {
			root = new NamespaceTreeNode(FileType.DIR, "",
					Constants.MASTER_USER, Constants.MKDIR_LENGTH, "/");
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
	public int addNode(String path, int replication, FileType type,
			String ipAddr) {

		if (path.equals("/"))
			return -2;
		String[] dirList = path.split("/");
		synchronized (root) {
			return traverseDir(root, dirList, 0, type, ipAddr);
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
	private int traverseDir(NamespaceTreeNode start, String[] dirList,
			int level, FileType type, String ipAddr) {

		if (level == dirList.length - 2) {

			boolean flag = false;
			for (NamespaceTreeNode node : start.getChildren()) {
				if (node.getInfo().equals(dirList[level + 1])) {
					flag = true;
					System.out.println("Already Exists"
							+ Arrays.toString(dirList));
					return -2;
				}
			}
			if (!flag) {
				String str = "";
				for (String s : dirList) {
					str += s + "/";
				}
				start.getChildren().add(
						new NamespaceTreeNode(type, dirList[level + 1], ipAddr,
								0, str.substring(0, str.length() - 1)));
				System.out.println("Dir added" + Arrays.toString(dirList));
				return 0;
			}

		}
		for (NamespaceTreeNode node : start.getChildren()) {
			if (node.getInfo().equals(dirList[level + 1])) {
				return traverseDir(node, dirList, ++level, type, ipAddr);
			}
		}
		System.out.println("Cannot create dir" + Arrays.toString(dirList));
		return -1;
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
			int level, FileType type, List<String> dataNodeList, long offset,
			String ipAddr, long fileLength) {

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
				String str = "";
				for (String s : dirList) {
					str += s + "/";
				}
				NamespaceTreeNode node = new NamespaceTreeNode(type,
						dirList[level + 1], dataNodeList, ipAddr, fileLength,
						str.substring(0, str.length() - 1));
				start.getChildren().add(node);
				blkId = node.addBlock(dataNodeList, offset);
			}

			return blkId;
		}
		for (NamespaceTreeNode node : start.getChildren()) {
			if (node.getInfo().equals(dirList[level + 1])) {
				return traverseFile(node, dirList, ++level, type, dataNodeList,
						offset, ipAddr, fileLength);
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
	public List<MetaData> listFiles(String sourcePath) {

		Queue<NamespaceTreeNode> queue = new LinkedList<>();
		queue.add(root);
		List<MetaData> list = new ArrayList<>();
		String nodes[] = sourcePath.split("/");

		boolean stop = false;

		if (nodes.length == 0) {
			for (NamespaceTreeNode temp : root.getChildren()) {
				list.add(new MetaData(temp.getInfo(), temp.getCreatedTime(),
						temp.getUser(), temp.getFileType(), temp.getSize()));
			}
			System.out.println("Listing files");
			return list;
		}

		while (!queue.isEmpty() && !stop) {

			NamespaceTreeNode node = queue.remove();
			if (node.getInfo().equals(nodes[nodes.length - 1])) {
				stop = true;
				for (NamespaceTreeNode temp : node.getChildren()) {
					list.add(new MetaData(temp.getInfo(),
							temp.getCreatedTime(), temp.getUser(), temp
									.getFileType(), temp.getSize()));
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
			long offset, String ipAddr, long fileLen) {
		synchronized (root) {
			String dirList[] = destinationPath.split("/");
			return traverseFile(root, dirList, 0, FileType.FILE, dataNodeList,
					offset, ipAddr, fileLen);
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

	private static String path = "";

	private static boolean dfsTraverse(String nodeVal, NamespaceTreeNode start,
			String blkId, int level) {

		if (start.getFileType() == FileType.FILE) {
			for (Block blk : start.getFileInfo().getBlocks()) {
				System.out.println("Inside for: " + blk.getBlockId());

				if (blk.getBlockId().equals(blkId)) {
					System.out.println("Receive blkId: " + blkId);
					System.out.println("My blkId: " + blk.getBlockId());
					System.out.println("My blkId: " + start.getPath());
					path = start.getPath();
					System.out.println("Replicate inside Path "
							+ start.getInfo() + "/");
					return true;
				}
			}
		}

		if (start.getChildren() != null) {

			for (NamespaceTreeNode node : start.getChildren()) {
				System.out.println("Replicate recursive " + node.getPath()
						+ "/ " + start.getPath());
				if (dfsTraverse(node.getInfo(), node, blkId, level + 1)) {
					return true;
				}
			}
		}

		return false;

	}

	public static String getReplicatePath(String blkId) {
		boolean builder = dfsTraverse("", root, blkId, 0);

		System.out.println("Replicate Path " + path);
		return path;
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

		NameSpaceTree tree = new NameSpaceTree();
		System.out.println(tree.addNode("/user", 3, FileType.DIR, "1"));
		System.out.println(tree.addNode("/user/kanth", 3, FileType.DIR, "2"));

		List<String> dataNodeList = new ArrayList<>();
		dataNodeList.add("1a");
		dataNodeList.add("2a");
		dataNodeList.add("3a");

		System.out.println(tree.put("/user/file2.txt", dataNodeList, 20, "1",
				20));

		System.out.println(tree.put("/user/kanth/file3.txt", dataNodeList, 20,
				"1", 20));

		System.out.println(tree.put("/user/file1.txt", dataNodeList, 20, "1",
				20));
		System.out.println(tree.put("/user/file1.txt", dataNodeList, 20, "1",
				20));
		System.out.println(tree.put("/user/file1.txt", dataNodeList, 20, "1",
				20));
		System.out.println(tree.put("/user/file1.txt", dataNodeList, 20, "1",
				20));

		System.out.println(tree.put("/user/file1.txt", dataNodeList, 20, "1",
				20));

		// System.out.println(tree.put("/user/kanth/file2.txt", dataNodeList,
		// 20,"2",20));

		List<String> blks = new ArrayList<>();
		List<BlocksMap> blkMap = tree.getBlockMap("/user/kanth/file3.txt");
		for (BlocksMap b : blkMap) {
			System.out.println(b.getBlk().getBlockId());
			System.out.println(getReplicatePath(b.getBlk().getBlockId()));
		}

		// ExecutorService service = Executors.newFixedThreadPool(2);
		// service.execute(new FSImage());

	}

	/***
	 * Traverse the Name space tree to delete and add
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

					/*
					 * System.out.println("BLockMap dfs NodeList: " +
					 * blkMap.getDatanodeInfo());
					 */
					return true;
				}
			}
		}

		if (start.getChildren() != null) {
			for (NamespaceTreeNode node : start.getChildren()) {
				return dfsTraverse(node, ipAddress, blkId, newIp);
			}
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
			NamespaceTreeNode.dataNodeBlockMap.put(ipAddress, blkIds);
			if (deleted) {
				List<String> newIpBlks = NamespaceTreeNode.dataNodeBlockMap
						.get(newIpAddress);
				if (newIpBlks == null)
					newIpBlks = new ArrayList<>();
				newIpBlks.add(blkId);
				// System.out.println("dataNodeBlockMap NodeList: " +
				// newIpBlks);
				NamespaceTreeNode.dataNodeBlockMap.put(newIpAddress, newIpBlks);
			}
		}
	}

	public static void initialTransferBlocks(String ipAddress) {
		List<String> blks = root.dataNodeBlockMap.get(ipAddress);
		for (String blk : blks) {
			Block block = NameNode.tree.getBlock(blk);

			if (block.getStatus().equals(BlockStatus.COMPLETED)) {
				List<String> dataNodes = new ArrayList<>(
						NamespaceTreeNode.blockDataMap.get(blk));
				List<String> newIpAddress = NameNode.getDiffNodeList(dataNodes);
				dataNodes.remove(ipAddress);
				for (String dataNode : dataNodes) {
					try {
						String ipAddr = newIpAddress.get(0);
						sendReply(dataNode, Constants.DATANODE_NAMENODE_PORT,
								blk, ipAddr);
						NameSpaceTree.update(blk, ipAddress,
								ipAddr);
						block.setStatus(BlockStatus.PROGRESS);
						block.setReplicateAckCount(block.getReplicateAckCount()-1);
						break;

					} catch (IOException err) {
						continue;
					}
				}
			}
		}
	}

	private static void sendReply(String dataNode, int datanodeNamenodePort,
			String blk, String ipAddr) throws UnknownHostException, IOException {
		Socket socket = new Socket(dataNode, datanodeNamenodePort);
		ObjectOutputStream oos = new ObjectOutputStream(
				socket.getOutputStream());
		String path = NameSpaceTree.getReplicatePath(blk);
		
		ReplicateMessage replicateMsg = new ReplicateMessage(
				RequestType.REPLICA, ipAddr,
				Constants.DATANODE_CLIENT_PORT, blk, path);
		oos.writeObject(replicateMsg);
		oos.close();
		socket.close();
		
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
					// System.out.println(NamespaceTreeNode.blockDataMap
					// .get(blkId));
					break;
				}
			}

		}
	}

}
