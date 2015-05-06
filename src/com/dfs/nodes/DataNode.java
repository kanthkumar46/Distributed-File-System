package com.dfs.nodes;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.messages.AckMessage;
import com.dfs.messages.BlockReportMessage;
import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class DataNode {
	private static InetAddress inetAddress;
	public static final int DATANODE_PORT;
	public static final String DATANODE_IP;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DATANODE_IP = inetAddress.getHostAddress();
		DATANODE_PORT = 8000;
	}
	
	private void init(){
		Timer heartBeatTimer = new Timer("HeartBeat", true);
		heartBeatTimer.schedule(new HeartBeat(), 0 , Constants.HEART_BEAT_TIME);
		
		Timer blockReportTimer = new Timer("BlockReport", true);
		blockReportTimer.schedule(new BlockReporter(), 0 , Constants.BLOCK_REPORT_TIME);
	}
	
	Runnable requestHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(DATANODE_PORT)){
				ExecutorService executor = Executors.newCachedThreadPool();
				while(true){
					Socket socket = servSock.accept();
					executor.submit(new DataNodeWorker(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		//dataNode.init();
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
		executor.submit(dataNode.requestHandler);
	} 
}

class HeartBeat extends TimerTask{
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			HeartBeatMessage msg = new HeartBeatMessage();
			stream.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
	}
}


class BlockReporter extends TimerTask{
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			BlockReportMessage msg = new BlockReportMessage();
			stream.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
	}
}

class DataNodeWorker implements Runnable{
	Socket client;
	public DataNodeWorker(Socket sock) {
		this.client = sock;
	}
	
	@Override
	public void run() {
		try(ObjectInputStream iStream = new ObjectInputStream(client.getInputStream())){
			RequestType reqType = (RequestType) iStream.readObject();
			System.out.println("Request Type :"+reqType.toString());
			if(reqType.equals(RequestType.PUT)){
				AckMessage ack = new AckMessage((String)iStream.readObject(),DataNode.DATANODE_IP);
				sendAckMessage(ack);
			}
			else if(reqType.equals(RequestType.GET)){
				
			}
			else if(reqType.equals(RequestType.REPLICA)){
				
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void sendAckMessage(AckMessage ack) {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.ACK_PORT_NUM);
		
		try(ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream())) {
			stream.writeObject(ack);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}