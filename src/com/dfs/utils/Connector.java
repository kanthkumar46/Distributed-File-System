package com.dfs.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

public class Connector {
private static final String MASTER_PATH = "master";
	
	public Socket connectToNameNode(int portNo){
		String nameNode = null;
		
		try(BufferedReader br = new BufferedReader(new InputStreamReader
				(new FileInputStream(new File(MASTER_PATH))))) {
			nameNode = br.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		Socket socket = null;
		try{
			socket = new Socket(nameNode, portNo);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return socket;
	}
	
	public void closeConnection(Socket socket){
		try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
