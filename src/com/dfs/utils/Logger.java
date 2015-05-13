package com.dfs.utils;

import java.io.File;
import java.util.List;

import com.dfs.messages.MetaData;

public class Logger {
	
	private static Logger logger = new Logger();
	
	private Logger(){}
	
	public static Logger getLogger(){
		return logger;
	}
	
	public void handleMkdirErrorCode(int errorCode, String dir_Path){
		if(errorCode == -1)
			System.err.println("Cannot create directory "+dir_Path);
		else if(errorCode == -2)
			System.err.println("Directory already exists "+dir_Path);
	}

	public void printDirectoryList(List<MetaData> list, String dir_path) {
		if(!Character.toString(dir_path.charAt(dir_path.length()-1))
				.equals(File.separator)){
			StringBuilder str = new StringBuilder(dir_path).append(File.separator);
			dir_path = str.toString();
		}
		
		if(list == null)
			System.err.println("Directory doesn't exists "+dir_path);
		else if(!list.isEmpty()){
			for(MetaData fileinfo : list){
				System.out.format("%-10s%-20s%-20s%-30s%-30s", fileinfo.getType(),fileinfo.getCreatedUser(),
						fileinfo.getSize(), fileinfo.getCreatedTime(), dir_path+fileinfo.getPath());
				System.out.print("\n");
			}
		}
	}

	public void handlePutRequestFailure(int errorCode) {
		if(errorCode == -1)
			System.err.println();
	}

	public void printUsage(int usageType) {
		switch (usageType) {
			case 0:System.err.println("No arguments passed");
			System.err.println("try: RIT-DFS -help");
			break;
			case 1:System.err.println("RIT-DFS -mkdir {directory name}");
			break;
			case 2:System.err.println("RIT-DFS -ls {directory name}");
			break;
			case 3:System.err.println("RIT-DFS -put {source-file-path} {dfs-file-path}");
			break;
			case 4:System.err.println("RIT-DFS -get {dfs-file-path} {target-file-path}");
			break;
			default:System.err.println("RIT-DFS -mkdir {directory name}");
			System.err.println("RIT-DFS -ls {directory name}");
			System.err.println("RIT-DFS -put {source-file-path} {dfs-file-path}");
			System.err.println("RIT-DFS -get {dfs-file-path} {target-file-path}");
			break;
		}
		System.exit(0);
	}
	
}
