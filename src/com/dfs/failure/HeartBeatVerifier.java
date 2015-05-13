package com.dfs.failure;

import java.util.List;

import com.dfs.nodes.NameSpaceTree;
import com.dfs.utils.Constants;

public class HeartBeatVerifier implements Runnable{

	@Override
	public void run() {
		
		while(true){
			
			List<SlaveList> list = HeartBeatHandler.slave;
			long curr = System.currentTimeMillis();
			for(SlaveList sl : list){
				if((curr - sl.date.getTime()) > Constants.HEART_BEAT_TIME){
					initiateBlockMovement(sl.ipAddress);
				}
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void initiateBlockMovement(String ipAddress) {
		NameSpaceTree.initialTransferBlocks(ipAddress);		
	}

}
