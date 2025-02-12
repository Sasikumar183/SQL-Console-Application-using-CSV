package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class InnerJoin {

    public static void join(String query) {
        int from = query.toUpperCase().indexOf("FROM");
        int join = query.toUpperCase().indexOf("JOIN");
        int on = query.toUpperCase().indexOf("ON");

        String leftTable = query.substring(from + 5, join).trim();
        String rightTable = query.substring(join + 5, on).trim();
        String condition = query.substring(on + 3).trim();

        String[] columns = query.substring(7, from).trim().split("\\s*,\\s*");
        String[] conditionParts = condition.split("\\s*=\\s*");

        List<String> leftCol = new ArrayList<>();
        List<String> rightCol = new ArrayList<>();

        int ord=0;
        Map<String,Integer> order = new HashMap<>();
        for (String s : columns) {
            if (s.trim().matches(leftTable + "\\..*")) { 
                leftCol.add(s.trim().substring(leftTable.length() + 1));
                order.put(s.trim().substring(leftTable.length() + 1), ord);
            }
            else if (s.trim().matches(rightTable + "\\..*")) {
                rightCol.add(s.trim().substring(rightTable.length() + 1));
                order.put(s.trim().substring(rightTable.length() + 1), ord);
            }
            ord++;
        }
        System.out.println(leftCol);
        File leftFile = new File(leftTable + ".csv");
        File rightFile = new File(rightTable + ".csv");

        if (!leftFile.exists() || !rightFile.exists()) {
            System.out.println("One or both tables not found.");
            return;
        }

        List<List<String>> leftTableData = readCSV(leftTable + ".csv");
        List<List<String>> rightTableData = readCSV(rightTable + ".csv");

        //System.out.println("\nLeft Table Data: " + leftTableData);
        //System.out.println("Right Table Data: " + rightTableData);

        String leftJoinColumn = conditionParts[1].trim();
        String rightJoinColumn = conditionParts[0].trim();

        if (leftJoinColumn.contains(".")) leftJoinColumn = leftJoinColumn.split("\\.")[1];
        if (rightJoinColumn.contains(".")) rightJoinColumn = rightJoinColumn.split("\\.")[1];
        

        List<List<String>> result = performInnerJoin(leftTableData, rightTableData, leftJoinColumn, rightJoinColumn, leftCol, rightCol);
        
        System.out.println(order);
        
        System.out.println("\nJoin Result:");
        displayTableInner(result,order);
    }

    
    public static List<List<String>> readCSV(String fileName) {
        List<List<String>> tableData = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
                if ((line = br.readLine()) != null) {
                List<String> headers = new ArrayList<>();
                for (String col : line.split(",")) {
                    headers.add(col.split(" ")[0].trim()); 
                }
                tableData.add(headers); 
            }
            
            while ((line = br.readLine()) != null) {
                tableData.add(Arrays.asList(line.split(",")));
            }
            
        } catch (IOException e) {
            System.out.println("Error reading file: " + fileName);
        }
        
        return tableData;
    }


    public static List<List<String>> performInnerJoin(List<List<String>> leftTableData, List<List<String>> rightTableData,
                                                 String rightJoinColumn,String leftJoinColumn,
                                                 List<String> leftCol, List<String> rightCol) {
        List<List<String>> result = new ArrayList<>(); 
        int leftIndex = leftTableData.get(0).indexOf(leftJoinColumn);
        int rightIndex = rightTableData.get(0).indexOf(rightJoinColumn);

        if (leftIndex == -1 || rightIndex == -1) {
            System.out.println("Error: Column not found in one of the tables.");
            return result; 
            }

        List<String> headerred= new ArrayList<>();
        for(String col:leftCol) {
          headerred.add(leftTableData.get(0).get(leftTableData.get(0).indexOf(col)));
        }
        
        for(String col:rightCol) {
            headerred.add(rightTableData.get(0).get(rightTableData.get(0).indexOf(col)));
        }
        result.add(headerred);
        for (int i = 1; i < leftTableData.size(); i++) {
            for (int j = 1; j < rightTableData.size(); j++) {
                if (leftTableData.get(i).get(leftIndex).equals(rightTableData.get(j).get(rightIndex))) {
                    List<String> joinedRow = new ArrayList<>();
                    for (String col : leftCol) {
                        int index = leftTableData.get(0).indexOf(col);
                        joinedRow.add(leftTableData.get(i).get(index));
                    }
                    for (String col : rightCol) {
                        int index = rightTableData.get(0).indexOf(col);
                        joinedRow.add(rightTableData.get(j).get(index));
                    }
                    result.add(joinedRow);
                }
            }
        }

        return result;
    }
    static void displayTableInner(List<List<String>> tableData, Map<String,Integer> order) {
    	List <Integer> actOrd=new ArrayList<>();
    	for(int i=0;i<tableData.get(0).size();i++) {
    		actOrd.add(order.get(tableData.get(0).get(i)));
    	}
    	System.out.println(actOrd);
    	
    	for (List<String> row : tableData) {
            List<String> rearrangedRow = new ArrayList<>();
            for (int index : actOrd) {
                rearrangedRow.add(row.get(index));
            }

            for (String s : rearrangedRow) {
                System.out.printf("%-27s |", s);
            }
            System.out.println();
        
        }
    }
   

}
