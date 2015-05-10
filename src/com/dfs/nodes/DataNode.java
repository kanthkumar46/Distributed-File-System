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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.dfs.blocks.BlockReport;
import com.dfs.messages.AckMessage;
import com.dfs.messages.ClientRequestMessage;
import com.dfs.messages.HeartBeatMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class DataNode {
	private static InetAddress inetAddress;
	public static final String DATANODE_IP;
	
	static {
		try {
			inetAddress = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DATANODE_IP = inetAddress.getHostAddress();
	}
	
	private void init(){
		//Timer heartBeatTimer = new Timer("HeartBeat", true);
		//heartBeatTimer.schedule(new HeartBeat(), 0 , Constants.HEART_BEAT_TIME);
		
		Timer blockReportTimer = new Timer("BlockReport", true);
		blockReportTimer.schedule(new BlockReporter(), 0 , Constants.BLOCK_REPORT_TIME);
	}
	
	Runnable requestHandler = new Runnable() {
		@Override
		public void run() {
			try(ServerSocket servSock = new ServerSocket(Constants.DATANODE_PORT)){
				ExecutorService executor = Executors.newCachedThreadPool();
				while(true){
					Socket socket = servSock.accept();
					executor.execute(new DataNodeWorker(socket));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	};
	
	public static void main(String[] args) {
		DataNode dataNode = new DataNode();
		dataNode.init();
		//BlockReporter.getBlockList("/Users/KanthKumar/Desktop/DFS/DATA");
		
		ExecutorService executor = Executors.newFixedThreadPool(1);
		executor.submit(dataNode.requestHandler);
	} 
}

class HeartBeat extends TimerTask{
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


class BlockReporter extends TimerTask{
	
	List<String> blockIds = new ArrayList<>();
	
	@Override
	public void run() {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.NAMENODE_BLOCK_PORT_NUM);
		
		try(ObjectOutputStream stream =  new ObjectOutputStream(socket.getOutputStream())){
			blockIds.clear();
			getBlockList(Constants.DATA_DIR);
			BlockReport msg = new BlockReport(blockIds, DataNode.DATANODE_IP);
			stream.writeObject(msg);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			connector.closeConnection(socket);
		}
	}

	private void getBlockList(String directoryName) {
		File directory = new File(directoryName);

	    File[] filesList = directory.listFiles();
	    if(filesList != null){
		    for (File file : filesList) {
		        if (file.isFile()) {
		            String BlockPath = file.getParent();
		            String blockId = BlockPath.substring
		            		(BlockPath.lastIndexOf(File.separator)+1);
		            blockIds.add(blockId);
		        } 
		        else
		        	getBlockList(file.getAbsolutePath());
		    }
	    }
	}
	
}

class DataNodeWorker implements Runnable{
	Socket client;
	public DataNodeWorker(Socket sock) {
		this.client = sock;
	}
	
	@Override
	public void run() {
		try(ObjectInputStream iStream = 
				new ObjectInputStream(client.getInputStream())){
			
			ClientRequestMessage reqMsg = (ClientRequestMessage) iStream.readObject();
			RequestType reqType = reqMsg.getRequestType();
			System.out.println("Request Type :"+reqType.toString());
			
			if(reqType.equals(RequestType.PUT)){
				writeBlockReceived(reqMsg);
				AckMessage ack = new AckMessage(reqMsg.getBlkId(),DataNode.DATANODE_IP);
				sendAckMessage(ack);
			}
			else if(reqType.equals(RequestType.GET)){
				try(FileInputStream fis = getBlockFile(reqMsg);
					Socket socket = new Socket(reqMsg.getIpAddress(),Constants.CLIENT_DATA_RECEIVE_PORT);
					GZIPOutputStream gzipOS = new 
							GZIPOutputStream(socket.getOutputStream());){
					
					byte[] buffer = new byte[1024];
					int len;
					while ((len = fis.read(buffer)) > 0) {
						gzipOS.write(buffer, 0, len);
					}
				}
			}
			else if(reqType.equals(RequestType.REPLICA)){
				writeBlockReceived(reqMsg);
				AckMessage ack = new AckMessage(reqMsg.getBlkId(),DataNode.DATANODE_IP);
				sendAckMessage(ack);
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	private void writeBlockReceived(ClientRequestMessage reqMsg) throws IOException {
		try(GZIPInputStream gzipIS = new GZIPInputStream(client.getInputStream());
			BufferedOutputStream blockFile = new 
						BufferedOutputStream(createBlockFile(reqMsg));){
				GZIPOutputStream replicateNode = replicateToOtherNode
						(reqMsg.getDataNodeList(),reqMsg);
				byte[] buffer = new byte[1024];
            	int len;
            	while((len = gzipIS.read(buffer)) > 0){
            		blockFile.write(buffer, 0, len);
            		if(replicateNode != null)
            			writeToPipeline(buffer,len,replicateNode);
            	}
            	if(replicateNode != null)
            		replicateNode.close();
			}
	}

	private GZIPOutputStream replicateToOtherNode(List<String> dataNodeList, ClientRequestMessage reqMsg) {
		dataNodeList.remove(0);
		if(dataNodeList.size() > 0){
			String dataNode = dataNodeList.get(0);
			try{
				Socket socket = new Socket(dataNode, Constants.DATANODE_PORT);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				String destPath = reqMsg.getDestinationPath();
				ClientRequestMessage replicateMsg = new ClientRequestMessage(DataNode.DATANODE_IP,
						Constants.DATANODE_PORT, reqMsg.getBlkId(), 
						destPath.substring(0, destPath.lastIndexOf('.'))+DataNode.DATANODE_IP+
						destPath.substring(destPath.lastIndexOf('.')),
						RequestType.REPLICA, dataNodeList);
				out.writeObject(replicateMsg);
				
				GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream());
				return gzipOS;
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private void writeToPipeline(byte[] buffer,int len,GZIPOutputStream replicateNodeStream) {
		System.out.println(len);
		try {
			replicateNodeStream.write(buffer, 0, len);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private FileOutputStream createBlockFile(ClientRequestMessage reqMsg) 
			throws FileNotFoundException{
		String destPath = reqMsg.getDestinationPath();
		String blockDirectory = Constants.DATA_DIR + destPath.substring(0, 
				destPath.lastIndexOf(File.separator)+1)+reqMsg.getBlkId();
		File file = new File(blockDirectory);
		file.mkdirs();
		String blockFilePath = blockDirectory+File.separator+
				destPath.substring(destPath.lastIndexOf(File.separator)+1);
		System.err.println("block path :"+blockFilePath);
		FileOutputStream fileOutputStream = new FileOutputStream(new File(blockFilePath));
		return fileOutputStream;
	}
	
	private FileInputStream getBlockFile(ClientRequestMessage reqMsg) 
			throws FileNotFoundException{
		String srcPath = reqMsg.getSourcePath();
		String chunkFilePath = Constants.DATA_DIR + srcPath.substring(0, 
				srcPath.lastIndexOf(File.separator)+1)+reqMsg.getBlkId()+File.separator+
				srcPath.substring(srcPath.lastIndexOf(File.separator)+1);
		System.err.println("block path :"+chunkFilePath);
		FileInputStream fileInputStream = new FileInputStream(new File(chunkFilePath));
		return fileInputStream;
	}
	
	private void sendAckMessage(AckMessage ack) {
		Connector connector = new Connector();
		Socket socket= connector.connectToNameNode(Constants.ACK_PORT_NUM);
		
		try(ObjectOutputStream stream = new ObjectOutputStream(socket.getOutputStream())) {
			stream.writeObject(ack);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}