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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.blocks.Block;
import com.dfs.blocks.BlockStatus;
import com.dfs.messages.AckMessage;
import com.dfs.messages.ListNameNodeReplyMessage;
import com.dfs.messages.Message;
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
class NameNodeClientRequest implements Runnable {

	@Override
	public void run() {

		try (ServerSocket serverSocket = new ServerSocket(Constants.PORT_NUM)) {
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectInputStream stream = new ObjectInputStream(
						socket.getInputStream());
				Message message = (Message) stream.readObject();
				ExecutorService service = Executors.newFixedThreadPool(5);
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

	private void get() {
		
		List<BlocksMap> blkMap = NameNode.tree.getBlockMap(message.getSourcePath());
		
	}

	private void put() {
		List<String> dataNodeList = NameNode.getNodeList(message.getReplication());
		String success = NameNode.tree.put(message.getDestinationPath(),dataNodeList);
		sendReply(new PutNameNodeReplyMessage(message.getSourcePath(),success,dataNodeList,message.getDestinationPath()),RequestType.PUT);
		
	}

	private void list() {
		
		List<String> fileList = NameNode.tree.listFiles(message
				.getDirectoryPath());
		sendReply(new ListNameNodeReplyMessage(fileList),RequestType.LIST);
	}

	/**
	 * Handling mkdir requests sent by client
	 */
	private void mkdir() {
		boolean out = NameNode.tree.addNode(message.getDirectoryPath(), 0,
				FileType.DIR);
		sendReply(new MkdirNameNodeReplyMessage(),RequestType.MKDIR);
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

class BlockReport implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
}

class BlockReportHandler implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
}

public class NameNode {

	private static List<String> nodeList;
	private static final String SLAVES_PATH = "slaves";
	private static final String RACK_AWARNSS_PATH = "rack_awareness.data";
	protected static NameSpaceTree tree;
	private static ArrayList<String> nodeToRackMapping;
	
	
	public static List<String> getNodeList(int num) {
		
		/*int size = nodeToRackMapping.size();
		Random rdm = new Random();
		int firstReplication = rdm.nextInt(size);
		int secondReplication = rdm.nextInt(size);
		int thirdReplication = rdm.nextInt(size);
		ArrayList<String> dataNodeList = new ArrayList<>();
		dataNodeList.add(nodeToRackMapping.get(firstReplication));
		dataNodeList.add(nodeToRackMapping.get(secondReplication));
		dataNodeList.add(nodeToRackMapping.get(thirdReplication));	*/
		ArrayList<String> dataNodeList = new ArrayList<>();
		dataNodeList.add("medusa.cs.rit.edu");
		dataNodeList.add("doors.cs.rit.edu");
		dataNodeList.add("buddy.cs.rit.edu");
		return dataNodeList;
	}

	public NameNode() throws IOException {
		tree = new NameSpaceTree();
		nodeToRackMapping = new ArrayList<>();
		readDataNodeList();
		readRackAwarness();
		
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
		//service.execute(new BlockReport());
	}

}
