package com.example.sql_in_csv;

import java.io.File;

public class ShowTables {
	public static void show(String query) {
		File folder= new File(".");
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".csv"));
        if(files.length==0) {
        	System.out.println("No Tables found");
        }
        else {
        	for(File file:files) {
        	System.out.println(file.getName().replace(".csv",""));
        }
        }
	}

}
