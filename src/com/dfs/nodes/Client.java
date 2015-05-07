package com.dfs.nodes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import com.dfs.messages.ClientRequestMessage;
import com.dfs.messages.ListNameNodeReplyMessage;
import com.dfs.messages.MkdirNameNodeReplyMessage;
import com.dfs.messages.PutNameNodeReplyMessage;
import com.dfs.utils.Constants;

public class Client {
	private static InetAddress inetAddress;
	public static final String CLIENT_IP;
	static ExecutorService executor = Executors.newFixedThreadPool(1);
	static int NO_OF_CHUNCKS;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		CLIENT_IP = inetAddress.getHostAddress();
	}
	
	Runnable replyHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(Constants.CLIENT_PORT_NUM)) {
				while(!Thread.currentThread().isInterrupted()){		
					Socket socket = servSock.accept();
					new clientWorker(socket).handleNameNodeReply();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}; 
	
	public static void main(String[] args) {
		String command = args[0];
		
		Client client = new Client();
		executor.execute(client.replyHandler);
		
		if(command.equals("-mkdir")){
			if(args.length != 2)
				System.err.println("print usage");
			else{
				System.out.println(args[1]);
				DFSCommand.mkdir(args[1]);
			}
		}
		else if(command.equals("-ls")){
			if(args.length != 2)
				System.err.println("print usage");
			else
				DFSCommand.ls(args[1]);
		}
		else if(command.equals("-put")){
			if(args.length != 3)
				System.err.println("print usage");
			else
				DFSCommand.put(args[1], args[2]);
		}
		else if(command.equals("-get")){
			if(args.length != 3)
				System.err.println("print usage");
			else
				DFSCommand.get(args[1], args[2]);
		}
		else{
			System.err.println(command + ": unknown command");
		}
		
		executor.shutdown();
	}
	
}

class clientWorker{
	Socket socket;
	public clientWorker(Socket sock) {
		this.socket = sock;
	}
	
	public void handleNameNodeReply() {
		try(ObjectInputStream iStream = new ObjectInputStream(socket.getInputStream())){
			RequestType reqType = (RequestType) iStream.readObject();
			System.out.println("Reply Type :"+reqType.toString());
			if(reqType.equals(RequestType.MKDIR)){
				MkdirNameNodeReplyMessage msg = (MkdirNameNodeReplyMessage)iStream.readObject();
				System.out.println(msg.getErrorCode());
				Client.executor.shutdownNow();
			}
			else if(reqType.equals(RequestType.LIST)){
				ListNameNodeReplyMessage msg = (ListNameNodeReplyMessage) iStream.readObject();
				System.out.println(msg.getFileList());
				Client.executor.shutdownNow();
			}
			else if (reqType.equals(RequestType.PUT)) {
				PutNameNodeReplyMessage msg = (PutNameNodeReplyMessage) iStream.readObject(); 
				transferBlockToDataNode(msg.getBlkId(),msg.getSourcePath(),msg.getDestinationPath(),msg.getDataNodeList());
				Client.NO_OF_CHUNCKS--;
				if(Client.NO_OF_CHUNCKS == 0)
					Client.executor.shutdownNow();
			}
			socket.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void transferBlockToDataNode(String blockId, String chunckPath, String destPath, List<String> dataNodeList) {
		System.err.println(blockId +"  "+chunckPath);
		System.err.println(dataNodeList);
		String dataNode = dataNodeList.get(0);
		System.err.println("transfer initiated to dataNode : "+dataNode);
		try(Socket socket = new Socket(dataNode, Constants.DATANODE_PORT);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			FileInputStream fis = new FileInputStream(new File(chunckPath));
			GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream())){
			
			String sourceFileName = chunckPath.split("_")[1];
			destPath = destPath.substring(0, destPath.lastIndexOf(File.separator));
			System.err.println(sourceFileName);
			
			ClientRequestMessage msg = new ClientRequestMessage(Client.CLIENT_IP, Constants.CLIENT_PORT_NUM, blockId, 
					sourceFileName, destPath, RequestType.PUT, dataNodeList);
			out.writeObject(msg);
			
			byte[] buffer = new byte[1024];
            int len;
			while((len = fis.read(buffer)) > 0){
				gzipOS.write(buffer, 0, len);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
