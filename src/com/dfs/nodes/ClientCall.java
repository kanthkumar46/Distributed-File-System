package com.dfs.nodes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ClientCall implements Runnable {

	int N;
	static ArrayList<String> list = new ArrayList<>();

	public ClientCall(int n) {
		this.N = n;
	}

	static ExecutorService service = Executors.newFixedThreadPool(10);

	public static void main(String[] args) throws UnknownHostException {

		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));
		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));
		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));
		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));
		list.add(new String("/user"));
		list.add(new String("/user/test"));
		list.add(new String("/test"));
		list.add(new String("/test"));
		list.add(new String("/test/test1"));
		list.add(new String("/test/phrrr"));
		list.add(new String("/test/od"));

		args = new String[5];

		//mkdirTest(args);
		listTest(args);
	}

	private static void listTest(String[] args) {
		synchronized (list) {
			for (int i = 0; i < 100; i++) {
				Random rnd = new Random();
				int r = rnd.nextInt(list.size());
				args[0] = "ls";
				args[1] = list.get(r);
				service.equals(new NameNodeReplyHandler(args));
				service.execute(new ClientCall(r));
			}
		}
		
	}

	private static void mkdirTest(String[] args) {
		synchronized (list) {
			for (int i = 0; i < 100; i++) {
				Random rnd = new Random();
				int r = rnd.nextInt(list.size());
				args[0] = "mkdir";
				args[1] = list.get(r);
				service.equals(new NameNodeReplyHandler(args));
				service.execute(new ClientCall(r));
			}
		}
	}

	@Override
	public void run() {

		//DFSCommand.mkdir(list.get(N));
		DFSCommand.ls(list.get(N));
	}
}
