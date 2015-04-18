package com.dfs.nodes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.messages.Message;

public class Client {
	private static final String MASTER_PATH = "master";
	private static InetAddress inetAddress;
	public static final String CLIENT_IP;
	public static final int CLIENT_PORT;
	private static final long CHUNK_SIZE = 64 * 1024 *1024;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		CLIENT_IP = inetAddress.getHostAddress();
		CLIENT_PORT = 8000;
	}
	
	Runnable reply = new Runnable() {
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
			else
				DFSCommand.mkdir(args[1]);
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
	
		/*RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(new File("tweets.csv"), "r");
		} catch (FileNotFoundException e) {
			System.out.println("source file not found");
			System.exit(0);
		}
		
		long fileLength = 0;
		try {
			fileLength = raf.length();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		int noOfChuncks = (int) Math.ceil((double)fileLength/CHUNK_SIZE);
		System.out.println(noOfChuncks);
		while(noOfChuncks != 0){
			readAndCreateChunk(raf,noOfChuncks);
			requestDataNodes();
			noOfChuncks--;
		}*/
	}

	private static void requestDataNodes() {
		String nameNode = null;
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader
				(new FileInputStream(new File(MASTER_PATH))))) {
			nameNode = br.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try(Socket socket = new Socket(nameNode, 5285)) {
			ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream());
			Message dataNodeRequest = new Message(CLIENT_IP,CLIENT_PORT,"sample.txt"," ",0,RequestType.PUT);
			stream.writeObject(dataNodeRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void readAndCreateChunk(RandomAccessFile raf,int i) {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File("chunk_"+i+".csv"));
		} catch (FileNotFoundException e) {
			System.out.println("cannot create a chuck file");
		}
		
		byte[] temp = new byte[1024];
		try {
			long remaining = CHUNK_SIZE;
			int bytesRead = 0;
			while(remaining!=0 && (bytesRead = raf.read(temp)) != -1){
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

class clientWorker implements Runnable{
	Socket socket;
	public clientWorker(Socket sock) {
		this.socket = sock;
	}
	
	@Override
	public void run() {
		
	}	
}


class Connector{
	private static final String MASTER_PATH = "master";
	
	public Socket connectToNameNode(){
		String nameNode = null;
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader
				(new FileInputStream(new File(MASTER_PATH))))) {
			nameNode = br.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		try(Socket socket = new Socket(nameNode, 5285)) {
			return socket;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}


class DFSCommand{
	public static int mkdir(String dir_path){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode();
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			Message makeDirectoryRequest = new Message(Client.CLIENT_IP,Client.CLIENT_PORT,
					dir_path,RequestType.MKDIR);
			stream.writeObject(makeDirectoryRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	public static int ls(String dir_path){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode();
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			Message listDirectoryRequest = new Message(Client.CLIENT_IP,Client.CLIENT_PORT,
					dir_path,RequestType.LIST);
			stream.writeObject(listDirectoryRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return 0;
	}
	
	public static int get(String sourcePath, String targetPath){
		return -1;
	}
	
	public static int put(String sourcePath, String destinationPath){
		return -1;
	}
}