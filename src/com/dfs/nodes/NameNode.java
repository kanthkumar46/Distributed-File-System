package com.dfs.nodes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.blocks.Block;
import com.dfs.blocks.BlockReportReceiver;
import com.dfs.blocks.BlockStatus;
import com.dfs.failure.FSImage;
import com.dfs.messages.AckMessage;
import com.dfs.messages.GetNameNodeReplyMessage;
import com.dfs.messages.ListNameNodeReplyMessage;
import com.dfs.messages.Message;
import com.dfs.messages.MetaData;
import com.dfs.messages.MkdirNameNodeReplyMessage;
import com.dfs.messages.NameNodeReplyMessage;
import com.dfs.messages.PutNameNodeReplyMessage;
import com.dfs.utils.Constants;

/***
 * To handle client requests. Server Socket running at port num 5285 Receives
 * requests such as list, mkdir, put, get
 * 
 * @author ssuman
 *
 */
class FSImageHandler implements Runnable {

	@Override
	public void run() {
		FSImage image = new FSImage();
		while(true){
			image.saveNameSpace();
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				
				e.printStackTrace();
			}
		}
		
	}
	
}
class NameNodeClientRequest implements Runnable {

	@Override
	public void run() {

		try (ServerSocket serverSocket = new ServerSocket(Constants.PORT_NUM)) {
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectInputStream stream = new ObjectInputStream(
						socket.getInputStream());
				Message message = (Message) stream.readObject();
				ExecutorService service = Executors.newFixedThreadPool(10);
				service.execute(new NameNodeHandler(message));
				
				socket.close();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

class NameNodeHandler implements Runnable {

	private Message message;

	
	public NameNodeHandler(Message message) {
		this.message = message;
	}

	@Override
	public void run() {

		decider();
	}

	private void decider() {

		if (message.getRequestType().equals(RequestType.MKDIR)) {
			mkdir();
		} else if (message.getRequestType().equals(RequestType.LIST)) {
			list();
		} else if(message.getRequestType().equals(RequestType.PUT)){
			put();
		} else if (message.getRequestType().equals(RequestType.GET)){
			get();
		}

	}
	
	
	/**
	 * Get request sent by dataNode will be handled here.
	 */
	private void get() {
		System.out.println(message.getDirectoryPath());
		List<BlocksMap> blkMap = NameNode.tree.getBlockMap(message.getDirectoryPath());
		sendReply(new GetNameNodeReplyMessage(blkMap),RequestType.GET);
		
	}

	/**
	 * Put request sent by datande
	 */
	private void put() {
		
		List<String> dataNodeList = NameNode.getNodeList(message.getReplication());
		String blkId = NameNode.tree.put(message.getDestinationPath(),dataNodeList,message.getBlockByteOffset(),
				message.getIpAddress(),message.getSourceFileLength());
		sendReply(new PutNameNodeReplyMessage(message.getSourcePath(),blkId,dataNodeList),RequestType.PUT);
		
	}

	private void list() {
		
		List<MetaData> fileList = NameNode.tree.listFiles(message
				.getDirectoryPath());
		sendReply(new ListNameNodeReplyMessage(fileList),RequestType.LIST);
	}

	/**
	 * Handling mkdir requests sent by client
	 */
	private void mkdir() {
		
		//System.out.println(message.getDirectoryPath());
		int out = NameNode.tree.addNode(message.getDirectoryPath(), 0,
				FileType.DIR,message.getIpAddress());
		
		sendReply(new MkdirNameNodeReplyMessage(out),RequestType.MKDIR);
	}

	/***
	 * Send reply to client through sockets. NameNodeReplyMessage object is
	 * sent.
	 * 
	 * @param obj
	 */
	private void sendReply(NameNodeReplyMessage obj,RequestType type) {
		try {
			Socket socket = new Socket(message.getIpAddress(),
					message.getPortNum());
			
			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());
			oos.writeObject(type);
			oos.writeObject(obj);
			oos.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

class DataNodeAckReceiver implements Runnable{
	
	public DataNodeAckReceiver(){
		
	}

	@Override
	public void run() {
		try (ServerSocket serverSocket = new ServerSocket(Constants.ACK_PORT_NUM)) {
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectInputStream stream = new ObjectInputStream(
						socket.getInputStream());
				AckMessage message = (AckMessage)stream.readObject();
				ExecutorService service = Executors.newFixedThreadPool(4);
				service.execute(new DataNodeAckHandler(message));
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		}finally {
			
		}
		
	}
}

class DataNodeAckHandler implements Runnable{

	private AckMessage ackMessage;
	
	public DataNodeAckHandler(AckMessage ackMessage){
		this.ackMessage = ackMessage;
	}
	@Override
	public void run() {
		String blkId = ackMessage.getBlockId();
		Block blk = NameNode.tree.getBlock(blkId);
		blk.setStatus(BlockStatus.COMPLETED);
		System.out.println(NameNode.tree.getBlock(blkId).getStatus());
	}
	
}

public class NameNode {

	private static List<String> nodeList;
	private static final String SLAVES_PATH = "slaves";
	private static final String RACK_AWARNSS_PATH = "rack_awareness.data";
	protected static NameSpaceTree tree;
	private static ArrayList<String> nodeToRackMapping;
	
	
	public static List<String> getNodeList(int num) {
		
		
		int size = nodeList.size();
		ArrayList<Integer> index = new ArrayList<>();
		for(int i=0;i<size;i++){
			index.add(i);
		}
		Collections.shuffle(index);
		//Integer[] list = index.subList(0, 3).toArray(new Integer[3]);
		ArrayList<String> dataNodeList = new ArrayList<>();
		dataNodeList.add(nodeList.get(index.get(0)));
		dataNodeList.add(nodeList.get(index.get(1)));
		dataNodeList.add(nodeList.get(index.get(2)));
		//ArrayList<String> dataNodeList = new ArrayList<>();
		//dataNodeList.add("ec2-52-24-230-154.us-west-2.compute.amazonaws.com");
		//dataNodeList.add("ec2-52-25-8-104.us-west-2.compute.amazonaws.com");
		//dataNodeList.add("ec2-52-25-9-19.us-west-2.compute.amazonaws.com");
		//dataNodeList.add("buddy.cs.rit.edu");
		return dataNodeList;
	}
	
	public static List<String> getDiffNodeList(List<String> ipAddress){
		List<String> node = new ArrayList<>(nodeList);
		node.removeAll(ipAddress);
		Collections.shuffle(node);
		return nodeList;
	}

	public NameNode() throws IOException {
		tree = new NameSpaceTree();
		nodeToRackMapping = new ArrayList<>();
		readDataNodeList();
		readRackAwarness();
		//getNodeList(3);
	}

	private void readRackAwarness() throws FileNotFoundException, IOException {
		// read rack awareness  file
		File file = new File(RACK_AWARNSS_PATH);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String node = null;
		while((node = reader.readLine())!= null)
			nodeToRackMapping.add(node.split(" ")[0]);
		reader.close();
	}

	private void readDataNodeList() throws FileNotFoundException, IOException {
		// Read Data node list from slaves file
		nodeList = new ArrayList<>();
		File file = new File(SLAVES_PATH);
		BufferedReader stream = new BufferedReader(new FileReader(file));
		String node = null;
		while ((node = stream.readLine()) != null)
			nodeList.add(node);
		stream.close();
	}

	public static void main(String[] args) throws IOException {
		NameNode node = new NameNode();
		ExecutorService service = Executors.newFixedThreadPool(5);
		service.execute(new NameNodeClientRequest());
		service.execute(new DataNodeAckReceiver());
		service.execute(new BlockReportReceiver());
		service.execute(new FSImageHandler());
	}

}
