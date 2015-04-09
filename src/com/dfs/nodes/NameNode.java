package com.dfs.nodes;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class NameNode {

	private List<String> nodeList ;
	private static final String SLAVES_PATH="slaves";
	private static final int defaultReplication =3;
	
	public NameNode() throws IOException{
		
		//Read Data node list from slaves
		nodeList = new ArrayList<>();
		File file = new File(SLAVES_PATH);
		BufferedReader stream = new BufferedReader(new FileReader(file));
		String node=null;
		while((node=stream.readLine())!=null)
			nodeList.add(node);
		stream.close();
		
		
	}
	
	
	public static void main(String[] args) throws IOException {
		NameNode node = new NameNode();
		
	}
	
	
}
