package com.dfs.nodes;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import com.dfs.utils.Logger;

class NameNodeReplyHandler implements Runnable {

	String[] args;

	public NameNodeReplyHandler(String parameters[]) {
		this.args = parameters;
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

	String[] args;
	List<BlocksMap> blockMap;
	CountDownLatch start, end;
	
	public DataNodeReplyHandler(String []parameters, List<BlocksMap> blockMap,
			CountDownLatch start, CountDownLatch end){
		this.args = parameters;
		this.blockMap = blockMap;
		this.start = start;
		this.end = end;
	}
	
	@Override
	public void run() {
		try (ServerSocket servSock = new ServerSocket(Constants.CLIENT_DATA_RECEIVE_PORT)) {
			start.countDown();
			Socket socket = servSock.accept();
			new clientWorker(socket, args).handleDateNodeReply();
			end.countDown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}

public class Client {
	private static InetAddress inetAddress;
	public static final String CLIENT_IP;
	public static ExecutorService executor = Executors.newFixedThreadPool(2);
	static int WRITE_CHUNCKS_COUNT;

	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		CLIENT_IP = inetAddress.getHostAddress();
	}

	public static void main(String[] args) throws IOException {
		if(args.length == 0){
			System.err.println("No arguments passed");
			Logger.getLogger().printUsage(0);
		}
			
		String command = args[0];
		executor.execute(new NameNodeReplyHandler(args));
		
		if (command.equals("-mkdir")) {
			if (args.length != 2)
				Logger.getLogger().printUsage(1);
			else
				DFSCommand.mkdir(args[1]);
		} 
		else if (command.equals("-ls")) {
			if (args.length != 2)
				Logger.getLogger().printUsage(2);
			else
				DFSCommand.ls(args[1]);
		} 
		else if (command.equals("-put")) {
			if (args.length != 3)
				Logger.getLogger().printUsage(3);
			else
				DFSCommand.put(args[1], args[2]);
		} 
		else if (command.equals("-get")) {
			if (args.length != 3)
				Logger.getLogger().printUsage(4);
			else
				DFSCommand.get(args[1], args[2]);
		} 
		else if(command.equals("-help")){
			Logger.getLogger().printUsage(5);
		}
		else {
			System.err.println(command + ": unknown command");
			System.err.println("try: RIT-DFS -help");
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
		try (ObjectInputStream iStream = new 
				ObjectInputStream(socket.getInputStream())) {
			
			RequestType reqType = (RequestType) iStream.readObject();
			//System.out.println("Reply Type :" + reqType.toString());
			
			if (reqType.equals(RequestType.MKDIR)) {
				MkdirNameNodeReplyMessage msg = (MkdirNameNodeReplyMessage) iStream
						.readObject();
				
				Logger.getLogger().handleMkdirErrorCode(msg.getErrorCode(),args[1]);
				Client.executor.shutdownNow();
			} 
			else if (reqType.equals(RequestType.LIST)) {
				ListNameNodeReplyMessage msg = (ListNameNodeReplyMessage) iStream
						.readObject();
				
				Logger.getLogger().printDirectoryList(msg.getFileList(),args[1]);
				Client.executor.shutdownNow();
			} 
			else if (reqType.equals(RequestType.PUT)) {
				PutNameNodeReplyMessage msg = (PutNameNodeReplyMessage) iStream
						.readObject();

				Logger.getLogger().handlePutRequestFailure(msg.getErrorCode());
				
				transferBlockToDataNode(msg.getBlkId(), msg.getChunkPath(),
						msg.getDataNodeList());

				Client.WRITE_CHUNCKS_COUNT--;
				if (Client.WRITE_CHUNCKS_COUNT == 0){
					Client.executor.shutdownNow();
					System.err.println("Done.");
				}
			} 
			else if (reqType.equals(RequestType.GET)) {
				GetNameNodeReplyMessage msg = (GetNameNodeReplyMessage) iStream
						.readObject();
				
				Logger.getLogger().handleGetRequestFailure(msg.getErrorCode());
				
				readBlocksFromDataNode(msg.getBlockMap());
				Client.executor.shutdownNow();
				System.err.println("Done.");
			}
			
			socket.close();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	

	public void handleDateNodeReply() {
		try(GZIPInputStream gzipIS = new GZIPInputStream(socket.getInputStream());
			BufferedOutputStream targetFile = new 
					BufferedOutputStream(createTargetFile())){
			byte[] buffer = new byte[1024];
        	int len;
        	while((len = gzipIS.read(buffer)) > 0){
        		targetFile.write(buffer, 0, len);
        	}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	private FileOutputStream createTargetFile() throws IOException{
		File tagetFile = new File(args[2]);
		if(!tagetFile.exists()) {
			tagetFile.createNewFile();
		}
		FileOutputStream fileOutputStream = new FileOutputStream(tagetFile, true);
		return fileOutputStream;
	}
	
	
	/***
	 * To read data from the dataNode. sending the request Try connecting to the
	 * first datanode in the list. if(unsuccessful) try connecting to 2nd
	 * datanode in the list if both fail try third. Else give up.
	 * 
	 * @param blockMap
	 *            Block to DataNode mapping sorted according to Byte offset.
	 * @throws IOException 
	 */
	private void readBlocksFromDataNode(List<BlocksMap> blockMap) 
			throws IOException {
		for (BlocksMap map : blockMap) {
			//System.out.println("Byte Offset :"+map.getBlk().getOffset());
			for (int i = 0; i < map.getDatanodeInfo().size(); i++) {
				//System.out.println("DataNode:"+map.getDatanodeInfo().get(i));
				try (Socket sock = new Socket(map.getDatanodeInfo().get(i),
						Constants.DATANODE_CLIENT_PORT)) {
					CountDownLatch begin = new CountDownLatch(1);
					CountDownLatch end = new CountDownLatch(1);
					new Thread(new DataNodeReplyHandler(args, blockMap, 
							begin,end)).start();
					begin.await();
					sendRequestToDataNode(map,sock);
					end.await();
					System.err.println("==========");
					break;
				} catch (IOException | InterruptedException err) {
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
			List<String> dataNodeList) {
		//System.err.println(blockId + "  " + chunckPath);
		//System.err.println(dataNodeList);
		String dataNode = dataNodeList.get(0);
		//System.err.println("transfer initiated to dataNode : " + dataNode);
		File localChunkfile = new File(chunckPath);
		try (Socket socket = new Socket(dataNode, Constants.DATANODE_CLIENT_PORT);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			FileInputStream fis = new FileInputStream(localChunkfile);) {
			
			ClientRequestMessage msg = new ClientRequestMessage
					(Client.CLIENT_IP, Constants.CLIENT_PORT_NUM, blockId,
					args[2], RequestType.PUT, dataNodeList);
			out.writeObject(msg);
			
			GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream());
			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) > 0) {
				gzipOS.write(buffer, 0, len);
			}
			gzipOS.close();
			localChunkfile.delete();
			
			System.err.print("==========");
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
