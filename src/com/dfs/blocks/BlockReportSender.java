package com.dfs.blocks;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;

import com.dfs.nodes.DataNode;
import com.dfs.utils.Connector;
import com.dfs.utils.Constants;

public class BlockReportSender extends TimerTask{
	
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
