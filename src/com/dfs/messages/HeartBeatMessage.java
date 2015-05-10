package com.dfs.messages;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class HeartBeatMessage {
	private String ipAddress;
	private long diskSpace;

	public HeartBeatMessage() {
		try {
			this.setIpAddress(InetAddress.getLocalHost().getHostAddress());
			this.setDiskSpace(new File(File.pathSeparator).getFreeSpace());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}

	public long getDiskSpace() {
		return diskSpace;
	}

	public void setDiskSpace(long diskSpace) {
		this.diskSpace = diskSpace;
	}

	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	
}
