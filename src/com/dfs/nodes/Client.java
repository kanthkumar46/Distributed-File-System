package com.dfs.nodes;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.dfs.messages.ClientRequestMessage;
import com.dfs.messages.GetNameNodeReplyMessage;
import com.dfs.messages.ListNameNodeReplyMessage;
import com.dfs.messages.MkdirNameNodeReplyMessage;
import com.dfs.messages.PutNameNodeReplyMessage;
import com.dfs.utils.Constants;

class NameNodeReplyHandler implements Runnable {

	String args[];

	public NameNodeReplyHandler(String args[]) {
		this.args = args;
	}

	@Override
	public void run() {
		try (ServerSocket servSock = new ServerSocket(Constants.CLIENT_PORT_NUM)) {
			while (!Thread.currentThread().isInterrupted()) {
				Socket socket = servSock.accept();
				new clientWorker(socket, args).handleNameNodeReply();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

class DataNodeReplyHandler implements Runnable {

	String [] parameters;
	
	
	public DataNodeReplyHandler(String []parameters){
		this.parameters = parameters;
	}
	
	
	@Override
	public void run() {
		try (ServerSocket servSock = new ServerSocket(Constants.CLIENT_PORT_NUM)) {
			Socket socket = servSock.accept();
			GZIPInputStream stream = new GZIPInputStream(socket.getInputStream());
			FileOutputStream outStream = createBlockFile();
			
			
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	private FileOutputStream createBlockFile() throws FileNotFoundException{
		String blockPath = parameters[2];
		File file = new File(blockPath);
		file.mkdirs();
		FileOutputStream fileOutputStream = new FileOutputStream(new File(blockPath));
		return fileOutputStream;
	}
	
}

public class Client {
	private static InetAddress inetAddress;
	public static final String CLIENT_IP;
	static ExecutorService executor = Executors.newFixedThreadPool(2);
	static int NO_OF_CHUNCKS;

	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		CLIENT_IP = inetAddress.getHostAddress();
	}

	/***
	 * Over head of sending path to namenode and namenode sending it back.
	 * 
	 */
	/*
	 * Runnable replyHandler = new Runnable() {
	 * 
	 * @Override public void run() { try(ServerSocket servSock = new
	 * ServerSocket(Constants.CLIENT_PORT_NUM)) {
	 * while(!Thread.currentThread().isInterrupted()){ Socket socket =
	 * servSock.accept(); new clientWorker(socket).handleNameNodeReply(); } }
	 * catch (IOException e) { e.printStackTrace(); } } };
	 */

	public static void main(String[] args) throws IOException {
		String command = args[0];
		executor.execute(new NameNodeReplyHandler(args));
		

		if (command.equals("-mkdir")) {
			if (args.length != 2)
				System.err.println("print usage");
			else {
				System.out.println(args[1]);
				DFSCommand.mkdir(args[1]);
			}
		} else if (command.equals("-ls")) {
			if (args.length != 2)
				System.err.println("print usage");
			else
				DFSCommand.ls(args[1]);
		} else if (command.equals("-put")) {
			if (args.length != 3)
				System.err.println("print usage");
			else
				DFSCommand.put(args[1], args[2]);
		} else if (command.equals("-get")) {
			if (args.length != 3)
				System.err.println("print usage");
			else
				DFSCommand.get(args[1], args[2]);
		} else {
			System.err.println(command + ": unknown command");
		}

		executor.shutdown();
	}

}

class clientWorker {
	Socket socket;
	String args[];

	public clientWorker(Socket sock, String[] args) {
		this.socket = sock;
		this.args = args;
	}

	public void handleNameNodeReply() {
		try (ObjectInputStream iStream = new ObjectInputStream(
				socket.getInputStream())) {
			RequestType reqType = (RequestType) iStream.readObject();
			System.out.println("Reply Type :" + reqType.toString());
			if (reqType.equals(RequestType.MKDIR)) {
				MkdirNameNodeReplyMessage msg = (MkdirNameNodeReplyMessage) iStream
						.readObject();
				System.out.println(msg.getErrorCode());
				Client.executor.shutdownNow();
			} else if (reqType.equals(RequestType.LIST)) {
				ListNameNodeReplyMessage msg = (ListNameNodeReplyMessage) iStream
						.readObject();
				System.out.println(msg.getFileList());
				Client.executor.shutdownNow();
			} else if (reqType.equals(RequestType.PUT)) {
				PutNameNodeReplyMessage msg = (PutNameNodeReplyMessage) iStream
						.readObject();

				transferBlockToDataNode(msg.getBlkId(), msg.getSourcePath(),
						msg.getDestinationPath(), msg.getDataNodeList());

				Client.NO_OF_CHUNCKS--;
				if (Client.NO_OF_CHUNCKS == 0)
					Client.executor.shutdownNow();
			} else if (reqType.equals(RequestType.GET)) {
				GetNameNodeReplyMessage msg = (GetNameNodeReplyMessage) iStream
						.readObject();
				readBlockFromDataNode(msg.getBlockMap());
				Client.executor.shutdown();
			}
			socket.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/***
	 * To read data from the dataNode. sending the request Try connecting to the
	 * first datanode in the list. if(unsuccessful) try connecting to 2nd
	 * datanode in the list if both fail try third. Else give up.
	 * 
	 * @param blockMap
	 *            Block to DataNode mapping sorted according to Byte offset.
	 */
	private void readBlockFromDataNode(List<BlocksMap> blockMap) {
		ExecutorService executor = Executors.newFixedThreadPool(2);
		executor.execute(new DataNodeReplyHandler(args));
		for (BlocksMap map : blockMap) {
			for (int i = 0; i < map.getDatanodeInfo().size(); i++) {
				try (Socket sock = new Socket(map.getDatanodeInfo().get(i),
						Constants.DATANODE_PORT)) {

					sendRequestToDataNode(map, sock);
					break;

				} catch (IOException err) {
					continue;
				}
			}
		}
	}

	/***
	 * Writes Client Request Object to DataNode. Sends the Block Id, source Path
	 * and the request Type.
	 * 
	 * @param map
	 *            Block Map -> mappping from block to datanode list
	 * @param socket
	 *            open socket connection.
	 * @throws IOException
	 */
	private void sendRequestToDataNode(BlocksMap map, Socket socket)
			throws IOException {
		ObjectOutputStream stream = new ObjectOutputStream(
				socket.getOutputStream());
		ClientRequestMessage clientRequestMessage = new ClientRequestMessage(
				Client.CLIENT_IP, Constants.CLIENT_PORT_NUM, map.getBlk()
						.getBlockId(), args[1], RequestType.GET);
		stream.writeObject(clientRequestMessage);
		stream.close();
	}

	
	private void transferBlockToDataNode(String blockId, String chunckPath,
			String destPath, List<String> dataNodeList) {
		System.err.println(blockId + "  " + chunckPath);
		System.err.println(dataNodeList);
		String dataNode = dataNodeList.get(0);
		System.err.println("transfer initiated to dataNode : " + dataNode);
		try (Socket socket = new Socket(dataNode, Constants.DATANODE_PORT);
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				FileInputStream fis = new FileInputStream(new File(chunckPath));
				GZIPOutputStream gzipOS = new GZIPOutputStream(
						socket.getOutputStream())) {

			String sourceFileName = chunckPath.split("_")[1];
			destPath = destPath.substring(0,
					destPath.lastIndexOf(File.separator));
			System.err.println(sourceFileName);

			ClientRequestMessage msg = new ClientRequestMessage(
					Client.CLIENT_IP, Constants.CLIENT_PORT_NUM, blockId,
					sourceFileName, destPath, RequestType.PUT, dataNodeList);
			out.writeObject(msg);

			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) > 0) {
				gzipOS.write(buffer, 0, len);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
