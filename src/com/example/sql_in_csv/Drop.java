package com.example.sql_in_csv;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Drop {
	public static void drop(String query) throws FileNotFoundException, IOException {
		String tableName = query.substring(10).trim();
        File file = new File(tableName + ".csv");

        if (!file.exists()) { 
            System.out.println("Table not found");
            return;
        }
        
        if(file.delete())
        	System.out.println("Table dropped from the databases");
        else
        	System.out.println("Error while dropping");
		

	}
}
