package com.dfs.blocks;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import com.dfs.utils.Constants;

public class BlockReportReceiver implements Runnable {

	
	public BlockReportReceiver(){
		
	}
	
	@Override
	public void run() {
		
		try(ServerSocket serverSocket = new ServerSocket(Constants.NAMENODE_BLOCK_PORT_NUM)){
			while(true){
				Socket socket = serverSocket.accept();
				
			}
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}

}

class BlockReportHandler implements Runnable {

	@Override
	public void run() {
		
		
	}
	
}
