package cs.bigdata.Tutorial2;


import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	public static List<String> count() throws IOException {
		
		String localSrc = "arbres.csv";
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		List<String> result = new ArrayList<>();
		
		try{
			InputStreamReader isr = new InputStreamReader(in);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			while (line !=null){
				// Process of the current line
				result.add(line);
				// go to the next line
				line = br.readLine();
			}
			br.close();
		} catch(IOException e) {
			result = null;
		}
		finally{
			//close the file
			in.close();
			fs.close();
		}
		
		return result;
	}

}
