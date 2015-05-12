package com.dfs.failure;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.TimerTask;

import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class HeartBeatSender extends TimerTask{
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
