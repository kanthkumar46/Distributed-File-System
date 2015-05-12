package com.dfs.nodes;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.dfs.messages.AckMessage;
import com.dfs.messages.ClientRequestMessage;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class DataNodeClientWorker implements Runnable{
	Socket client;
	
	public DataNodeClientWorker(Socket sock) {
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
				writeReceivedBlock(reqMsg);
				AckMessage ack = new AckMessage(reqMsg.getBlkId(),DataNode.DATANODE_IP);
				sendAckMessage(ack);
			}
			else if(reqType.equals(RequestType.GET)){
				transferBlockToClient(reqMsg);
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	private void writeReceivedBlock(ClientRequestMessage reqMsg) 
			throws IOException {
		try(GZIPInputStream gzipIS = new GZIPInputStream(client.getInputStream());
			BufferedOutputStream blockFile = new 
						BufferedOutputStream(createBlockFile(reqMsg));){
				GZIPOutputStream replicateNode = initiateTransfeToOtherNode
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

	private void transferBlockToClient(ClientRequestMessage reqMsg) 
			throws IOException{
		try(BufferedInputStream bis = new BufferedInputStream(getBlockFile(reqMsg));
			Socket socket = new Socket(reqMsg.getIpAddress(),
					Constants.CLIENT_DATA_RECEIVE_PORT);
			GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream());){
				
			byte[] buffer = new byte[1024];
			int len;
			while ((len = bis.read(buffer)) > 0) {
				gzipOS.write(buffer, 0, len);
			}
		}
	}
	
	private GZIPOutputStream initiateTransfeToOtherNode(List<String> dataNodeList, 
			ClientRequestMessage reqMsg) {
		dataNodeList.remove(0);
		if(dataNodeList.size() > 0){
			String dataNode = dataNodeList.get(0);
			try{
				Socket socket = new Socket(dataNode, Constants.DATANODE_CLIENT_PORT);
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				ClientRequestMessage replicateMsg = new ClientRequestMessage(DataNode.DATANODE_IP,
						Constants.DATANODE_CLIENT_PORT, reqMsg.getBlkId(), 
						reqMsg.getDestinationPath(), RequestType.PUT, dataNodeList);
				out.writeObject(replicateMsg);
				
				GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream());
				return gzipOS;
			}catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	private void writeToPipeline(byte[] buffer,int len,
			GZIPOutputStream replicateNodeStream) {
		try {
			replicateNodeStream.write(buffer, 0, len);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private GZIPOutputStream createBlockFile(ClientRequestMessage reqMsg) 
			throws IOException{
		String destPath = reqMsg.getDestinationPath();
		//destPath = destPath.replace("/", File.separator);
		
		String blockDirectory = Constants.DATA_DIR + destPath.substring(0, 
				destPath.lastIndexOf(File.separator)+1)+reqMsg.getBlkId();
		File file = new File(blockDirectory);
		file.mkdirs();
		String blockFilePath = blockDirectory+File.separator+
				destPath.substring(destPath.lastIndexOf(File.separator)+1);
		System.err.println("block path :"+blockFilePath);
		GZIPOutputStream gzOutputStream = new GZIPOutputStream
				(new FileOutputStream(new File(blockFilePath)))/*{
			{
				def.setLevel(Deflater.BEST_SPEED);
			}
		}*/;
		return gzOutputStream;
	}
	
	private GZIPInputStream getBlockFile(ClientRequestMessage reqMsg) 
			throws IOException{
		String srcPath = reqMsg.getSourcePath();
		//srcPath = srcPath.replace("/", File.separator);
		
		String chunkFilePath = Constants.DATA_DIR + srcPath.substring(0, 
				srcPath.lastIndexOf(File.separator)+1)+reqMsg.getBlkId()+File.separator+
				srcPath.substring(srcPath.lastIndexOf(File.separator)+1);
		System.err.println("block path :"+chunkFilePath);
		GZIPInputStream gzInputStream = new GZIPInputStream
				(new FileInputStream(new File(chunkFilePath)));
		return gzInputStream;
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