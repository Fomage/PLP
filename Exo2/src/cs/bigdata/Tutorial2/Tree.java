package cs.bigdata.Tutorial2;

import java.io.IOException;
import java.util.List;

public class Tree {
	
	public static void main(String[] args) {
		List<String> data=null;
		try {
			data = CompterLigneFile.count();
		} catch (IOException e) {
			data = null;
		}
		
		if(data == null) {
			System.out.println("Error opening arbres.csv");
		} else {
			
			for(String t : data.subList(1, data.size())) {
				String[] split = t.split(";");
				int year = 0;
				double height = 0.0;
				try {
					year = Integer.parseInt(split[5]);
				} catch(NumberFormatException e) {
					
				}
				try {
					height = Double.parseDouble(split[6]);
				} catch(NumberFormatException e) {
					
				}
				System.out.println(String.format("Hauteur: %f, Année de plantation: %d", height, year));
			}
		}
	}
}
