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
		
		
	}
	
}

class NameNodeClientReply implements Runnable {

	private Message message;
	public NameNodeClientReply(Message message){
		this.message = message;
	}
	
	@Override
	public void run() {
		try {
			Socket socket = new Socket(message.getIpAddress(),message.getPortNum());
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			//oos.writeObject(obj);
		} catch (IOException e) {
			
		}
		
	}
}

public class NameNode {

	
	private static List<String> nodeList;
	private static final String SLAVES_PATH = "slaves";
	private static final int defaultReplication = 3;
	
	public List<String> getNodeList(int num){
		return null;
	}
	
	
	public NameNode() throws IOException {

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
