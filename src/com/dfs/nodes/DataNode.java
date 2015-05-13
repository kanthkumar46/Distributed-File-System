package com.dfs.nodes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.blocks.BlockReportSender;
import com.dfs.failure.HeartBeatSender;
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
		Timer heartBeatTimer = new Timer("HeartBeat", true);
		heartBeatTimer.schedule(new HeartBeatSender(), 0 , Constants.HEART_BEAT_TIME);
		
		Timer blockReportTimer = new Timer("BlockReport", true);
		blockReportTimer.schedule(new BlockReportSender(), 0 , Constants.BLOCK_REPORT_TIME);
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
