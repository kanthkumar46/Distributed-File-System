package com.dfs.nodes;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientCall implements Runnable{
	
	int N;
	static ArrayList<String> list = new ArrayList<>();
	
	
	public ClientCall(int n){
		this.N = n;
	}
	static ExecutorService service = Executors.newFixedThreadPool(10);
	
	public static void main(String[] args) {
		
		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));
		service.execute(new NameNodeReplyHandler(args));
		
		for(int i=0;i<40;i++){
			
			service.execute(new ClientCall(i%2));
		}
	}

	@Override
	public void run() {
		Random rand = new Random();
		int ran = rand.nextInt(list.size());
		DFSCommand.mkdir(list.get(ran));
	}
}
