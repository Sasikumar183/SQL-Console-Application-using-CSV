package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class AlterTable {
    
    public static void alterTable( String query) {
        query = query.substring(11).trim();
    	String tableName =query.split(" ")[0];

        int addInd = query.toUpperCase().indexOf("ADD");
        int dropInd = query.toUpperCase().indexOf("DROP COLUMN");
        int alterInd = query.toUpperCase().indexOf("ALTER COLUMN");
        
        String filePath = tableName + ".csv";

        if (dropInd != -1) {
            String columnToDrop = query.substring(dropInd + 11).trim();
            dropColumn(filePath, columnToDrop);
        } else if (addInd != -1) {
            String columnToAdd = query.substring(addInd + 3).trim();
            addColumn(filePath, columnToAdd);
        } else if (alterInd != -1) {
            String columnToModify = query.substring(alterInd + 12).trim();
            modifyColumn(filePath, columnToModify);
        } else {
            System.out.println("Error: Invalid ALTER TABLE query.");
        }
    }

    private static void addColumn(String filePath, String columnName) {
        List<List<String>> tableData = readCSV(filePath);
        if (tableData.isEmpty()) {
            System.out.println("Table not found.");
            return;
        }
        for(int i=0;i<tableData.get(0).size();i++) {
        	//System.out.println(tableData.get(0).get(i).split(" ")[0]+" "+columnName.split(" ")[0]);
        	if(tableData.get(0).get(i).split(" ")[0].equals(columnName.split(" ")[0])) {
        		System.out.println("Column already exists.");
                return;
        	}
        }
        
        if(columnName.toUpperCase().contains("NOT NULL") && tableData.size()>1) {
        	System.out.println("NOT NULL Column cannot be added to the table which has the data");
        	return;
        }
        if(columnName.toUpperCase().contains("PRIMARY KEY") && tableData.size()>1) {
        	System.out.println("PRIMARY KEY Column cannot be added to the table which has the data");
        	return;
        }
        tableData.get(0).add(columnName);
        for (int i = 1; i < tableData.size(); i++) {
            tableData.get(i).add("null");
        }

        writeCSV(filePath, tableData);
        System.out.println("Column added successfully.");
    }

    private static void dropColumn(String filePath, String columnName) {
    	//System.out.println(columnName);
    	
        List<List<String>> tableData = readCSV(filePath);
        if (tableData.isEmpty()) {
            System.out.println("Table not found.");
            return;
        }
        //System.out.println(tableData.get(0));
        List<String> tableHead=new ArrayList<>();
        for(int i=0;i<tableData.get(0).size();i++) {
        	tableHead.add(tableData.get(0).get(i).split(" ")[0]);
        	if(tableData.get(0).get(i).split(" ")[0].equals(columnName)) {
        		if(tableData.get(0).get(i).toUpperCase().contains("PRIMARY KEY")) {
        			System.out.println("Primary key column cannot be dropped");
        			return;
        		}
        		
        	}
        	
        }
        
        int colIndex = tableHead.indexOf(columnName);
        if (colIndex == -1) {
            System.out.println("Column not found.");
            return;
        }
        if(tableHead.size()==1) {
        	System.out.println("No Other Columns Exist So won't drop the column");
        	return;
        }

        for (List<String> row : tableData) {
            row.remove(colIndex);
        }

        writeCSV(filePath, tableData);
        System.out.println("Column dropped successfully.");
    }

    private static void modifyColumn(String filePath, String columnInfo) {
        String[] parts = columnInfo.split(" ");
        if (parts.length < 2) {
            System.out.println("Invalid column modification syntax.");
            return;
        }

        String columnName = parts[0];
        String newType = parts[1];

        List<List<String>> tableData = readCSV(filePath);
        if (tableData.isEmpty()) {
            System.out.println("Table not found.");
            return;
        }
        System.out.println(tableData.get(0));
        int found=0;
        for (int i = 0; i < tableData.get(0).size(); i++) {
            if (tableData.get(0).get(i).split(" ")[0].equalsIgnoreCase(columnName)) {
                tableData.get(0).set(i, columnInfo); // Update column definition
                found=1;
                break;
            }
        }

        if (found==0) {
            System.out.println("Column not found.");
            return;
        }
        writeCSV(filePath, tableData);
        System.out.println("Column datatype modified to: " + newType);
    }

    static List<List<String>> readCSV(String filePath) {
        List<List<String>> tableData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                tableData.add(new ArrayList<>(Arrays.asList(line.split(","))));
            }
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
        return tableData;
    }

    private static void writeCSV(String filePath, List<List<String>> rows) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(filePath))) {
        	for (int i = 0; i < rows.size(); i++) {
        	    bw.write(String.join(",", rows.get(i)));
        	    if (i < rows.size() - 1) {
        	        bw.newLine();
        	    }
        	}
        } catch (IOException e) {
            System.out.println("Error writing file: " + e.getMessage());
        }
    }
}
