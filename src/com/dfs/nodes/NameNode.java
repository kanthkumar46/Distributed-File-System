package com.dfs.nodes;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class NameNode {

	List<String> nodeList ;
	
	public NameNode() throws IOException{
		
		//Read Data node list from slaves
		nodeList = new ArrayList<>();
		File file = new File("slaves");
		BufferedReader stream = new BufferedReader(new FileReader(file));
		String node=null;
		while((node=stream.readLine())!=null)
			nodeList.add(node);
		stream.close();
		
		
	}
	
	
}
