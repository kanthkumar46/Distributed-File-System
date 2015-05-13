package com.dfs.nodes;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.dfs.messages.ClientRequestMessage;
import com.dfs.messages.ReplicateMessage;
import com.dfs.utils.Constants;

public class DataNodeNameNodeWorker implements Runnable {
	Socket nameNode;
	
	public DataNodeNameNodeWorker(Socket socket) {
		this.nameNode = socket;
	}

	@Override
	public void run() {
		try(ObjectInputStream iStream = 
				new ObjectInputStream(nameNode.getInputStream())){
			ReplicateMessage replicateMsg = (ReplicateMessage) iStream.readObject();
			RequestType reqType = replicateMsg.getType();
			System.out.println("Request Type :"+reqType.toString());

			replicateBlockToOtherNode(replicateMsg);

		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void replicateBlockToOtherNode(ReplicateMessage replicateMsg) 
			throws UnknownHostException, IOException {
		try(BufferedInputStream bis = new BufferedInputStream(getBlockFile(replicateMsg));
			Socket socket = new Socket(replicateMsg.getIpAddress(),
					Constants.DATANODE_CLIENT_PORT);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());){
			
				List<String> dataNodeList = new ArrayList<String>();
				dataNodeList.add(replicateMsg.getIpAddress());
				ClientRequestMessage msg = new ClientRequestMessage (DataNode.DATANODE_IP, 
						Constants.DATANODE_CLIENT_PORT, replicateMsg.getBlkId(),
						replicateMsg.getBlkPath(), RequestType.PUT,dataNodeList);
				out.writeObject(msg);
				
				GZIPOutputStream gzipOS = new GZIPOutputStream(socket.getOutputStream());	
				byte[] buffer = new byte[1024];
				int len;
				while ((len = bis.read(buffer)) > 0) {
					gzipOS.write(buffer, 0, len);
				}
				gzipOS.close();
			}
	}
	
	private InputStream getBlockFile(ReplicateMessage replicateMsg) 
			throws FileNotFoundException, IOException {
		String blockPath = replicateMsg.getBlkPath();
		//blockPath = blockPath.replace("/", File.separator);
		
		String blockDirectory = Constants.DATA_DIR + blockPath.substring(0, 
				blockPath.lastIndexOf(File.separator)+1) + replicateMsg.getBlkId();
		blockPath = blockDirectory+File.separator+
				blockPath.substring(blockPath.lastIndexOf(File.separator)+1);
		
		System.err.println("block path :"+blockPath);
		
		GZIPInputStream gzInputStream = new GZIPInputStream
				(new FileInputStream(new File(blockPath)));
		return gzInputStream;
	}

}
