package com.dfs.failure;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;




import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Constants;

public class HeartBeatReceiver implements Runnable{

	@Override
	public void run() {
		try (ServerSocket serverSocket = new ServerSocket(
				Constants.HEART_BEAT_PORT)) {
			while (true) {
				Socket socket = serverSocket.accept();
				ObjectInputStream stream = new ObjectInputStream(
						socket.getInputStream());
				HeartBeatMessage hbMessage = (HeartBeatMessage) stream.readObject();
				ExecutorService service = Executors.newFixedThreadPool(1);
				service.execute(new HeartBeatHandler(hbMessage));
			}
		} catch (IOException | ClassNotFoundException e) {
			
			e.printStackTrace();
		} 
		
	}

}

class HeartBeatHandler implements Runnable {

	private HeartBeatMessage message;
	protected static List<SlaveList> slave = new ArrayList<>();
	
	
	public HeartBeatHandler(HeartBeatMessage hbMessage) {
		this.setMessage(hbMessage);
		
	}

	private void updateList(HeartBeatMessage message) {

		boolean flag = false;
		synchronized (slave) {
			for (SlaveList list : slave) {
				if (list.ipAddress.equals(message.getIpAddress())) {
					flag = true;
					list.date = new Date();
				}
			}
			if (flag == false) {
				slave.add((new SlaveList(message.getIpAddress(), new Date())));
			}
		}

	}

	@Override
	public void run() {
		
		
	}

	public HeartBeatMessage getMessage() {
		return message;
	}

	public void setMessage(HeartBeatMessage message) {
		this.message = message;
	}
	
}
