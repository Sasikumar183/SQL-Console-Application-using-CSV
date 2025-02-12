package com.example.sql_in_csv;

import java.util.Scanner;



public class Main {
	public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
        	
			while (true) {
				System.out.println("Enter the SQL Query");
			    String query=scanner.nextLine();
			    
			    try {
					    if(query.toUpperCase().startsWith("CREATE TABLE")) {
					        CreateTable.create(query);
					        //System.out.println(database);
					    }
					    else if(query.toUpperCase().startsWith("INSERT INTO")) {
					        InsertData.insert(query);
					    }
					    
					    else if(query.toUpperCase().startsWith("DELETE FROM")) {
					        DeleteData.delete(query);
					    }
					    else if(query.toUpperCase().startsWith("UPDATE ")) {
					        Update.update(query);
					    }
					    else if(query.toUpperCase().contains("LEFT JOIN")) {
					    	LeftJoin.join(query);
					    }
					    else if(query.toUpperCase().contains("RIGHT JOIN")) {
					    	RightJoin.join(query);
					    }
					    else if(query.toUpperCase().contains("FULL JOIN")) {
					    	FullJoin.join(query);
					    }
					    else if(query.toUpperCase().contains("JOIN")||query.toUpperCase().contains("INNER JOIN")) {
					        InnerJoin.join(query);
					    }
					    else if(query.toUpperCase().startsWith("SELECT ")) {
					        SelectTable.select(query);
					    }
					    else if(query.toUpperCase().startsWith("TRUNCATE TABLE")) {
					        Truncate.truncate(query);
					    }
					    else if(query.toUpperCase().startsWith("DROP ")) {
					        Drop.drop(query);
					    }
					    else if(query.toUpperCase().startsWith("DESC ")) {
					    	TableDesc.description(query.substring(5).trim());
					    }
					    else if(query.toUpperCase().startsWith("ALTER TABLE")) {
					    	AlterTable.alterTable(query);
					    }
					    else if(query.toUpperCase().startsWith("SHOW TABLE")) {
					    	ShowTables.show(query);
					    }
					    else {
					    	System.out.println("Check Syntax");
					    }
					
				} 
			    catch (Exception e) {
					e.printStackTrace();
				}
			   
			}
		}
    }
}
