package com.example.sql_in_csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class TableDesc {
	public static void description(String query) throws IOException {
		
		File tableFile = new File(query+".csv");
		
		if (!tableFile.exists()) {
			System.out.println("Table not found");
			return;
		}
		
		try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
			String line[] =br.readLine().split(",");
			if (line.length==0)
				System.out.println("No columns in the table");
			else {
				System.out.printf("%-15s | %-15s | %-6s | %-10s | %-12s |\n", "FIELD", "TYPE", "NULL", "PRIMARY", "AUTO_INC");
			    System.out.println("----------------------------------------------------------------------------");
				for(int i=0;i<line.length;i++) {
					String column=line[i].split(" ")[0];
					String type=line[i].split(" ")[1];
					String PRI=" ",isNull="YES",isAutoInc=" ";
					if (line[i].toUpperCase().contains("NOT NULL")) isNull ="NO";
					if (line[i].toUpperCase().contains("PRIMARY KEY")) {
						PRI= "PRI";
						isNull="NO";
					}
			        if (line[i].toUpperCase().contains("AUTO_INCREMENT")) {
			        	isAutoInc = "auto_increment";
			        	isNull="NO";
			        }
			        System.out.printf("%-15s | %-15s | %-6s | %-10s | %-12s |\n",
			                column, // Field Name
			                type, // Type
			                isNull, // NULL Allowed
			                PRI, // Primary Key
			                isAutoInc // Auto Increment
			        );
				}
			}
		}
	
		
		
		
	}

}
