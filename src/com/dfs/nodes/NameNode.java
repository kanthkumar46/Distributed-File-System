package com.dfs.nodes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.messages.Message;
import com.dfs.messages.NameNodeReplyMessage;


/***
 * To handle client requests. Server Socket running at port num 5285
 * Receives requests such as list, mkdir, put, get
 * @author ssuman
 *
 */
class NameNodeClientRequest implements Runnable {

	private final int PORT_NUM = 5285;

	@Override
	public void run() {

		try (ServerSocket serverSocket = new ServerSocket(PORT_NUM)) {
			Socket socket = serverSocket.accept();
			ObjectInputStream stream = new ObjectInputStream(
					socket.getInputStream());
			Message message = (Message) stream.readObject();
			ExecutorService service = Executors.newFixedThreadPool(5);
			service.execute(new NameNodeHandler(message));
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}

	}

}

class NameNodeHandler implements Runnable {

	private Message message;
	
	
	public NameNodeHandler(Message message){
		this.message = message;
	}
	
	@Override
	public void run() {
		
		decider();
	}

	/***
	 * Decides the type of request. 
	 */
	private void decider() {
		
		if(message.getRequestType().equals(RequestType.MKDIR)){
			mkdir();
		}
		else if(message.getRequestType().equals(RequestType.LIST)){
			list();
		}
		
	}

	private void list() {
		List<String> fileList = NameNode.tree.listFiles(message.getSourcePath());
		
	}

	/**
	 * Handling mkdir requests sent by client
	 */
	private void mkdir() {
		boolean out =NameNode.tree.addNode(message.getSourcePath(), 0, FileType.DIR);
		sendReply(new NameNodeReplyMessage(null,null,out==true?0:-1,RequestType.MKDIR));
	}
	
	/***
	 * Send reply to client through sockets. NameNodeReplyMessage object is sent.
	 * @param obj
	 */
	private void sendReply(Object obj){
		try {
			Socket socket = new Socket(message.getIpAddress(),message.getPortNum());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(obj);
			oos.close();
			socket.close();
		} catch (IOException e) {	
			e.printStackTrace();
		}
		
	}
	
}

public class NameNode {

	
	private static List<String> nodeList;
	private static final String SLAVES_PATH = "slaves";
	private static final int defaultReplication = 3;
	private static final String RACK_AWARNSS_PATH="rack_awareness.data";
	protected static NameSpaceTree tree ;
	public List<String> getNodeList(int num){
		return null;
	}
	
	
	public NameNode() throws IOException {
		tree = new NameSpaceTree();
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
	}

}
