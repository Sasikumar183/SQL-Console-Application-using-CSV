package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class RightJoin {

    public static void join(String query) {
        int from = query.toUpperCase().indexOf("FROM");
        int join = query.toUpperCase().indexOf("RIGHT JOIN");
        int on = query.toUpperCase().indexOf("ON");
        int where = query.toUpperCase().indexOf("WHERE");

        String leftTable = query.substring(from + 5, join).trim();
        String rightTable = query.substring(join + 11, on).trim();
        String condition = query.substring(on + 3, where == -1 ? query.length() : where).trim();
        String whereCond[] = null;

        if (where != -1) {
            whereCond = query.trim().substring(where + 6).split(" and ");
        }

        List<String> leftColWhe = new ArrayList<>();
        List<String> rightColWhe = new ArrayList<>();

        if (whereCond != null) {
            for (String con : whereCond) {
                if (con.startsWith(leftTable + ".")) {
                    leftColWhe.add(con.substring(leftTable.length() + 1));
                } else if (con.startsWith(rightTable + ".")) {
                    rightColWhe.add(con.substring(rightTable.length() + 1));
                } else {
                    System.out.println("Table Not Found in WHERE clause");
                }
            }
        }
        System.out.println(leftColWhe+"  "+rightColWhe);

        String[] columns = query.substring(7, from).trim().split("\\s*,\\s*");
        String[] conditionParts = condition.split("\\s*=\\s*");

        List<String> leftCol = new ArrayList<>();
        List<String> rightCol = new ArrayList<>();
        File leftFile = new File(leftTable + ".csv");
        File rightFile = new File(rightTable + ".csv");

        if (!leftFile.exists() || !rightFile.exists()) {
            System.out.println("One or both tables not found.");
            return;
        }

        List<List<String>> leftTableData = InnerJoin.readCSV(leftTable + ".csv");
        List<List<String>> rightTableData = InnerJoin.readCSV(rightTable + ".csv");
        
        Map<String,Integer> order = new HashMap<>();

        if(columns.length==1 && columns[0].equals("*")) {
        	int ord=0;
        	for(String s:leftTableData.get(0)) {
        		leftCol.add(s);
                order.put(s.trim(), ord);
                ord++;
                
        	}
        	for(String s:rightTableData.get(0)) {
        		rightCol.add(s);
                order.put(s.trim(), ord);
                ord++;
        	}
        	
        }
        else 
        {
	        int ord=0;
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
        }


        String leftJoinColumn = conditionParts[1].trim();
        String rightJoinColumn = conditionParts[0].trim();

        if (leftJoinColumn.contains(".")) leftJoinColumn = leftJoinColumn.split("\\.")[1];
        if (rightJoinColumn.contains(".")) rightJoinColumn = rightJoinColumn.split("\\.")[1];

        List<List<String>> result = performRightJoin(leftTableData, rightTableData, leftJoinColumn, rightJoinColumn, leftCol, rightCol, leftColWhe, rightColWhe);

        System.out.println("\nJoin Result:");
        InnerJoin.displayTableInner(result, order);
    }

    public static List<List<String>> performRightJoin(
    	    List<List<String>> leftTableData, List<List<String>> rightTableData,
    	    String leftJoinColumn, String rightJoinColumn,
    	    List<String> leftCol, List<String> rightCol,
    	    List<String> leftColWhe, List<String> rightColWhe) {

    	    List<List<String>> result = new ArrayList<>();

    	    int leftIndex = leftTableData.get(0).indexOf(leftJoinColumn);
    	    int rightIndex = rightTableData.get(0).indexOf(rightJoinColumn);

    	    if (leftIndex == -1 || rightIndex == -1) {
    	        System.out.println("Error: Column not found in one of the tables.");
    	        return result;
    	    }

    	    // Construct the header row
    	    List<String> headerRow = new ArrayList<>();
    	    headerRow.addAll(leftCol);
    	    headerRow.addAll(rightCol);
    	    result.add(headerRow);

    	    for (int j = 1; j < rightTableData.size(); j++) {
    	        boolean found = false;

    	        for (int i = 1; i < leftTableData.size(); i++) {
    	            if (rightTableData.get(j).get(rightIndex).equals(leftTableData.get(i).get(leftIndex))) {
    	                List<String> joinedRow = new ArrayList<>();
    	                found = true;

    	                // Add left table data
    	                for (String col : leftCol) {
    	                    int index = leftTableData.get(0).indexOf(col);
    	                    joinedRow.add(index != -1 ? leftTableData.get(i).get(index) : null);
    	                }

    	                // Add right table data
    	                for (String col : rightCol) {
    	                    int index = rightTableData.get(0).indexOf(col);
    	                    joinedRow.add(index != -1 ? rightTableData.get(j).get(index) : null);
    	                }

    	                // Apply WHERE conditions
    	                if (InnerJoin.passesWhereConditions(leftTableData, rightTableData, i, j, leftColWhe, rightColWhe)) {
    	                    result.add(joinedRow);
    	                }
    	            }
    	        }

    	        // If no match was found, insert NULL for left table
    	        if (!found) {
    	            List<String> joinedRow = new ArrayList<>(Collections.nCopies(leftCol.size(), null));

    	            for (String col : rightCol) {
    	                int index = rightTableData.get(0).indexOf(col);
    	                joinedRow.add(index != -1 ? rightTableData.get(j).get(index) : null);
    	            }

    	            if (InnerJoin.passesWhereConditions(leftTableData, rightTableData, -1, j, leftColWhe, rightColWhe)) {
    	                result.add(joinedRow);
    	            }
    	        }
    	    }

    	    return result;
    	}

}
