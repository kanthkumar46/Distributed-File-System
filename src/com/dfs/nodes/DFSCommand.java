package com.dfs.nodes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;

import com.dfs.messages.Message;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

class DFSCommand{
	
	public static int mkdir(String dir_path){
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			Message makeDirectoryRequest = new Message(Client.CLIENT_IP, Constants.CLIENT_PORT_NUM,
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
			Message listDirectoryRequest = new Message(Client.CLIENT_IP, Constants.CLIENT_PORT_NUM,
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
		Client.NO_OF_CHUNCKS = noOfChuncks;
		System.err.println(noOfChuncks);
		
		while(noOfChuncks != 0){
			File tempDir = new File("temp");
			if(!tempDir.exists())
				tempDir.mkdir();
			String chunckPath = "temp//chunk"+noOfChuncks+"_"+sourceFile.getName();
			long chunkByteOffset = readAndCreateChunk(ra_SourceFile,chunckPath);
			requestDataNodes(socket,chunckPath,destinationPath,chunkByteOffset);
			noOfChuncks--;
		}
	
		return 0;
	}
	
	private static void requestDataNodes(Socket socket,String chunckPath, String destinationPath, long chunkByteOffset) {
		try(ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream())) {
			Message dataNodeRequest = new Message(Client.CLIENT_IP, Constants.CLIENT_PORT_NUM,
					chunckPath, destinationPath, 0, RequestType.PUT, chunkByteOffset);
			stream.writeObject(dataNodeRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static long readAndCreateChunk(RandomAccessFile ra_SourceFile,String chunckPath) {
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(new File(chunckPath));
		} catch (IOException e) {
			System.out.println("cannot create a chuck file");
		}
		
		byte[] temp = new byte[1024];
		long byteOffset = 0;
		try {
			long remaining = Constants.CHUNK_SIZE;
			int bytesRead = 0;
			byteOffset = ra_SourceFile.getFilePointer();
			while(remaining!=0 && (bytesRead = ra_SourceFile.read(temp)) != -1){
				fos.write(temp,0,bytesRead);
				remaining -= 1024;	
			}
			fos.flush();
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return byteOffset;
	}
	
}
