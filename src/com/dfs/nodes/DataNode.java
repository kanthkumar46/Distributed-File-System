package com.dfs.nodes;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import com.dfs.blocks.BlockReport;
import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class DataNode {
	private static InetAddress inetAddress;
	public static final String DATANODE_IP;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DATANODE_IP = inetAddress.getHostAddress();
	}
	
	private void init(){
		//Timer heartBeatTimer = new Timer("HeartBeat", true);
		//heartBeatTimer.schedule(new HeartBeat(), 0 , Constants.HEART_BEAT_TIME);
		
		Timer blockReportTimer = new Timer("BlockReport", true);
		blockReportTimer.schedule(new BlockReporter(), 0 , Constants.BLOCK_REPORT_TIME);
	}
	
	Runnable ClientRequestHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(Constants.DATANODE_CLIENT_PORT)){
				ExecutorService executor = Executors.newCachedThreadPool();
				while(true){
					Socket socket = servSock.accept();
					executor.execute(new DataNodeClientWorker(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};
	
	Runnable NameNodeRequestHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(Constants.DATANODE_NAMENODE_PORT)){
				ExecutorService executor = Executors.newCachedThreadPool();
				while(true){
					Socket socket = servSock.accept();
					executor.execute(new DataNodeNameNodeWorker(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		dataNode.init();
		
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.submit(dataNode.ClientRequestHandler);
		executor.submit(dataNode.NameNodeRequestHandler);
	} 
}


class HeartBeat extends TimerTask{
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket = connector.connectToNameNode(Constants.PORT_NUM);
		
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
	
	List<String> blockIds = new ArrayList<>();
	
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.NAMENODE_BLOCK_PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			blockIds.clear();
			getBlockList(Constants.DATA_DIR);
			BlockReport msg = new BlockReport(blockIds, DataNode.DATANODE_IP);
			stream.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
	}

	private void getBlockList(String directoryName) {
		File directory = new File(directoryName);

	    File[] filesList = directory.listFiles();
	    if(filesList != null){
		    for (File file : filesList) {
		        if (file.isFile()) {
		            String BlockPath = file.getParent();
		            String blockId = BlockPath.substring
		            		(BlockPath.lastIndexOf(File.separator)+1);
		            blockIds.add(blockId);
		        } 
		        else
		        	getBlockList(file.getAbsolutePath());
		    }
	    }
	}
	
}
