package com.dfs.failure;

import java.util.Date;

public class SlaveList {

	
	protected String ipAddress;
	protected int portNum;
	protected Date date;
	
	public SlaveList(String ipAddr,Date date){
		this.ipAddress=ipAddr;
		this.date=date;
	}
	
}
