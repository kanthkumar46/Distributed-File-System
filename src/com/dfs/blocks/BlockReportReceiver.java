package com.dfs.blocks;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dfs.messages.ReplicateMessage;
import com.dfs.nodes.NameNode;
import com.dfs.nodes.NameSpaceTree;
import com.dfs.nodes.NamespaceTreeNode;
import com.dfs.nodes.RequestType;
import com.dfs.utils.Constants;

public class BlockReportReceiver implements Runnable {
	
	
	@Override
	public void run() {
		
		try(ServerSocket serverSocket = new ServerSocket(Constants.NAMENODE_BLOCK_PORT_NUM)){
			while(true){
				Socket socket = serverSocket.accept();
				ObjectInputStream stream = new ObjectInputStream(socket.getInputStream());
				BlockReport blkReport = (BlockReport) stream.readObject();
				ExecutorService service = Executors.newFixedThreadPool(1);
				service.execute(new BlockReportHandler(blkReport));
				
			}
		} catch (IOException | ClassNotFoundException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		List<String> blkIds = new ArrayList<>();
		blkIds.add("A");
		blkIds.add("B");
		blkIds.add("C");
		blkIds.add("D");
		
		BlockReport report = new BlockReport(blkIds,"123");
		
		BlockReportHandler blockReportHandler = new BlockReportHandler(report);
		
		List<String> nameNodeBlkIds = new ArrayList<>();
		nameNodeBlkIds.add("A");
		nameNodeBlkIds.add("B");
		nameNodeBlkIds.add("C");
		nameNodeBlkIds.add("D");
		for(String s:blockReportHandler.findMissingBlocks(nameNodeBlkIds)){
			System.out.println(s);
		}
		
	}

}

class BlockReportHandler implements Runnable {
	
	private BlockReport report;
	
	public BlockReportHandler(BlockReport blkReport){
		this.setReport(blkReport);
	}

	@Override
	public void run() {
		List<String> blocks =NamespaceTreeNode.dataNodeBlockMap.get(report.getIpAddress());
		if(blocks == null)
			blocks = new ArrayList<>();
		
		ArrayList<String> finalList = findMissingBlocks(blocks);
		
		for(String blkId : finalList){
			List<String> dataNodes = new ArrayList<>(NamespaceTreeNode.blockDataMap.get(blkId));
			dataNodes.remove(report.getIpAddress());
			for(String dataNode : dataNodes){
				try{
					//TODO: 
					List<String> newIpAddress = NameNode.getDiffNodeList(dataNodes);
					for(String ipAddr : newIpAddress){
						System.out.println("New Ip Address:"+ newIpAddress);
						sendReply(dataNode,Constants.DATANODE_NAMENODE_PORT,blkId,ipAddr);
						NameSpaceTree.update(blkId,report.getIpAddress(),ipAddr);
						break;
					}
				}catch(IOException err){
					continue;
				}
			}
		}
	}

	public ArrayList<String> findMissingBlocks(List<String> blocks) {
		Map<String,String> notPresent = new HashMap<>();
		for(String blkId : blocks){
			notPresent.put(blkId, blkId);
			System.out.println("Block Id from NameNode:"+ blkId);
		}
		
		for(String blkId: report.getBlkId()){
			notPresent.remove(blkId);
			System.out.println("Block Id from DataNode:"+ blkId);
		}
		
		ArrayList<String> finalList = new ArrayList<>();
		for(Map.Entry<String, String> entry:notPresent.entrySet()){
			finalList.add(entry.getKey());
		}
		return finalList;
	}

	public BlockReport getReport() {
		return report;
	}

	public void setReport(BlockReport report) {
		this.report = report;
	}
	
	private void sendReply(String ipAddress, int portNum,String blkId,String toIpAddress) throws UnknownHostException, IOException {
		
			Socket socket = new Socket(ipAddress,
					portNum);
			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());
			String path =NameSpaceTree.getReplicatePath(blkId);
			ReplicateMessage replicateMsg = new ReplicateMessage(RequestType.REPLICA,toIpAddress
					,Constants.DATANODE_CLIENT_PORT,blkId,path);
			oos.writeObject(replicateMsg);
			oos.close();
			socket.close();
	}
}
