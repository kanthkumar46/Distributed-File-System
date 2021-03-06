package com.dfs.utils;

import java.io.File;
import java.nio.file.Paths;

public class Constants {
	public static final long HEART_BEAT_TIME = 5 * 1000; // 5 seconds
	public static final long BLOCK_REPORT_TIME = 20 * 1000; // 20 seconds

	public static final int PORT_NUM = 5285;
	public static final int RACK_SIZE = 3;
	public static final int DEFAULT_REPLICATION =3;
	
	public static final long CHUNK_SIZE = 64 * 1024 *1024; // 64 MB
	public static final int ACK_PORT_NUM = 5275;
	public static final int DEFAULT_REPLICATION_FACTOR = 3;
	
	public static  final int HEART_BEAT_PORT = 9999;
	public static final long MKDIR_LENGTH = 0;
	
	public static final String MASTER_USER = "admin";
	
	public static final String DATA_DIR = System.getProperty("user.home")+File.separator+
											"DFS"+File.separator+"DATA";
	
	public static final int CLIENT_PORT_NUM = 8010;
	public static final int CLIENT_DATA_RECEIVE_PORT = 8030;
	public static final int CLINET_ACK_PORT_NUM = 8020;
	
	public static final int DATANODE_CLIENT_PORT = 9000;
	public static final int DATANODE_NAMENODE_PORT = 9090;
	public static final int NAMENODE_BLOCK_PORT_NUM = 5286;
	public static final int NAMENODE_HEARTBEAT_PORT_NUM = 5287;
	public static final String NAMENODE_IMAGE_DIR= Paths.get(System.getProperty("user.home"), "DFS","namenode").toString();
	public static final String NAMENODE_HIST_DIR = Paths.get(System.getProperty("user.home"),"DFS","tmp").toString();
}
