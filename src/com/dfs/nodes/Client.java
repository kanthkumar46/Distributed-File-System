package com.dfs.nodes;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import com.dfs.messages.ClientPutRequestMessage;
import com.dfs.messages.ListNameNodeReplyMessage;
import com.dfs.messages.Message;
import com.dfs.messages.MkdirNameNodeReplyMessage;
import com.dfs.messages.PutNameNodeReplyMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class Client {
	private static InetAddress inetAddress;
	public static final String CLIENT_IP;
	public static final int CLIENT_PORT;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		CLIENT_IP = inetAddress.getHostAddress();
		CLIENT_PORT = 8000;
	}
	
	Runnable replyHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(CLIENT_PORT)) {
				ExecutorService executor = Executors.newCachedThreadPool();
				while(true){
					Socket socket = servSock.accept();
					executor.submit(new clientWorker(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}; 
	
	public static void main(String[] args) {
		String command = args[0];
		
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
		
		Client client = new Client();
		ExecutorService executor = Executors.newFixedThreadPool(1);
		executor.submit(client.replyHandler);

	}
	
}

class clientWorker implements Runnable{
	Socket socket;
	public clientWorker(Socket sock) {
		this.socket = sock;
	}
	
	@Override
	public void run() {
		try(ObjectInputStream iStream = new ObjectInputStream(socket.getInputStream())){
			RequestType reqType = (RequestType) iStream.readObject();
			System.out.println("Reply Type :"+reqType.toString());
			if(reqType.equals(RequestType.MKDIR)){
				MkdirNameNodeReplyMessage msg = (MkdirNameNodeReplyMessage)iStream.readObject();
				System.out.println(msg.getErrorCode());
			}
			else if(reqType.equals(RequestType.LIST)){
				ListNameNodeReplyMessage msg = (ListNameNodeReplyMessage) iStream.readObject();
				System.out.println(msg.getFileList());
			}
			else if (reqType.equals(RequestType.PUT)) {
				PutNameNodeReplyMessage msg = (PutNameNodeReplyMessage) iStream.readObject(); 
				transferBlockToDataNode(msg.getBlkId(),msg.getSourcePath(),msg.getDestinationPath(),msg.getDataNodeList());
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void transferBlockToDataNode(String blockId, String chunckPath, String destPath, List<String> dataNodeList) {
		System.err.println(blockId +"  "+chunckPath);
		System.err.println(dataNodeList);
		String dataNode = dataNodeList.get(0);
		System.err.println("transfer initiated to dataNode : "+dataNode);
		try(Socket socket = new Socket(dataNode, 8000);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			FileInputStream fis = new FileInputStream(new File(chunckPath));
			GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream())){
			ClientPutRequestMessage msg = new ClientPutRequestMessage(Client.CLIENT_IP, Client.CLIENT_PORT, blockId, 
					destPath, RequestType.PUT, dataNodeList);
			out.writeObject(msg);
			byte[] buffer = new byte[1024];
            int len;
			while((len = fis.read(buffer)) != -1){
				gzipOS.write(buffer, 0, len);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

class DFSCommand{
	
	public static int mkdir(String dir_path){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			Message makeDirectoryRequest = new Message(Client.CLIENT_IP,Client.CLIENT_PORT,
					dir_path,RequestType.MKDIR);
			stream.writeObject(makeDirectoryRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
		
		return 0;
	}
	
	public static int ls(String dir_path){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			Message listDirectoryRequest = new Message(Client.CLIENT_IP,Client.CLIENT_PORT,
					dir_path,RequestType.LIST);
			stream.writeObject(listDirectoryRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
		
		return 0;
	}
	
	public static int get(String sourcePath, String targetPath){
		return 0;
	}
	
	public static int put(String sourcePath, String destinationPath){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		RandomAccessFile ra_SourceFile = null;
		long fileLength = 0;
		File sourceFile = new File(sourcePath);
		try {
			ra_SourceFile = new RandomAccessFile(sourceFile, "r");
			fileLength = ra_SourceFile.length();
		} catch (IOException e) {
			System.out.println("source file not found");
			System.exit(0);
		}
		
		int noOfChuncks = (int) Math.ceil((double)fileLength/Constants.CHUNK_SIZE);
		System.err.println(noOfChuncks);
		
		while(noOfChuncks != 0){
			File tempDir = new File("temp");
			if(!tempDir.exists())
				tempDir.mkdir();
			String chunckPath = "temp//chunk"+noOfChuncks+"_"+sourceFile.getName();
			readAndCreateChunk(ra_SourceFile,chunckPath);
			requestDataNodes(socket,chunckPath,destinationPath);
			noOfChuncks--;
		}
		
		return 0;
	}
	
	private static void requestDataNodes(Socket socket,String chunckPath, String destinationPath) {
		try(ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream())) {
			Message dataNodeRequest = new Message(Client.CLIENT_IP, Client.CLIENT_PORT,
					chunckPath,destinationPath,0,RequestType.PUT);
			stream.writeObject(dataNodeRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void readAndCreateChunk(RandomAccessFile ra_SourceFile,String chunckPath) {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(chunckPath));
		} catch (IOException e) {
			System.out.println("cannot create a chuck file");
		}
		
		byte[] temp = new byte[1024];
		try {
			long remaining = Constants.CHUNK_SIZE;
			int bytesRead = 0;
			while(remaining!=0 && (bytesRead = ra_SourceFile.read(temp)) != -1){
				fos.write(temp,0,bytesRead);
				remaining -= 1024;
			}
			
			fos.flush();
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}