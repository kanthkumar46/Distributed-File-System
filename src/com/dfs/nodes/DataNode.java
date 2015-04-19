package com.dfs.nodes;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

import com.dfs.messages.BlockReportMessage;
import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class DataNode {
	
	private void init(){
		Timer heartBeatTimer = new Timer("HeartBeat", true);
		heartBeatTimer.schedule(new HeartBeat(), 0 , Constants.HEART_BEAT_TIME);
		
		Timer blockReportTimer = new Timer("BlockReport", true);
		blockReportTimer.schedule(new BlockReporter(), 0 , Constants.BLOCK_REPORT_TIME);
	}
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		dataNode.init();
	} 
}

class HeartBeat extends TimerTask{
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode();
		
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
		Socket socket= connector.connectToNameNode();
		
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