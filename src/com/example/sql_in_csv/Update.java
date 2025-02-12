package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class Update {
    public static void update(String query) {
        query = query.trim();
        int setIndex = query.toUpperCase().indexOf("SET");
        int whereIndex = query.toUpperCase().indexOf("WHERE");

        if (setIndex == -1) {
            System.out.println("SET condition is missing.");
            return;
        }

        String tableName = query.substring(7, setIndex).trim();
        String setPart = (whereIndex != -1) ? query.substring(setIndex + 3, whereIndex).trim() : query.substring(setIndex + 3).trim();
        String wherePart = (whereIndex != -1) ? query.substring(whereIndex + 5).trim() : null;

        File file = new File(tableName + ".csv");
        if (!file.exists()) {
            System.out.println("Table not found.");
            return;
        }

        List<String[]> tableData = new ArrayList<>();
        String[] headers = null;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean isFirstRow = true;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                if (isFirstRow) {
                    headers = row;
                    isFirstRow = false;
                }
                tableData.add(row);
            }
        } catch (IOException e) {
            System.out.println("Error reading table: " + e.getMessage());
            return;
        }

        if (headers == null) {
            System.out.println("Table structure is empty.");
            return;
        }

        Map<String, Integer> columnIndices = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            columnIndices.put(headers[i].trim().split(" ")[0], i);
        }

        String[] setConditions = setPart.split(",");
        if (!validateConditions(setConditions, columnIndices)) {
            System.out.println("Invalid SET conditions. Must be in column=value format.");
            return;
        }

        String[] whereConditions = (wherePart != null) ? wherePart.split("AND") : new String[0];
        if (whereConditions.length > 0 && !validateConditions(whereConditions, columnIndices)) {
            System.out.println("Invalid WHERE conditions. Must be in column=value, column>value, or column<value format.");
            return;
        }

        int updatedRows = 0;

        for (int row = 1; row < tableData.size(); row++) {
            String[] rowData = tableData.get(row);
            boolean match = (whereConditions.length == 0) || evaluateConditions(whereConditions, columnIndices, rowData);

            if (match) {
                int end=applySetConditions(setConditions, columnIndices, rowData,tableData);
                if(end==-1)
                	return;
                updatedRows++;
            }
        }

        if (updatedRows > 0) {
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            	for (int i = 0; i < tableData.size(); i++) {
            	    bw.write(String.join(",", tableData.get(i)));
            	    if (i < tableData.size() - 1) {
            	        bw.newLine();
            	    }
            	}
                System.out.println(updatedRows + " row(s) updated successfully.");
            } catch (IOException e) {
                System.out.println("Error writing table: " + e.getMessage());
            }
        } else {
            System.out.println("No rows matched the conditions.");
        }
    }

    private static boolean validateConditions(String[] conditions, Map<String, Integer> columnIndices) {
    	System.out.println(columnIndices);
        for (String condition : conditions) {
//        	System.out.println(condition);
            String operator = condition.contains("=") ? "=" : condition.contains("<") ? "<" : condition.contains(">") ? ">" : null;
            if (operator == null) return false;

            String[] parts = condition.split(operator);
            if (parts.length != 2 || !columnIndices.containsKey(parts[0].trim())) return false;
        }
        return true;
    }

    private static boolean evaluateConditions(String[] whereConditions, Map<String, Integer> columnIndices, String[] rowData) {
        for (String condition : whereConditions) {
            condition = condition.trim();
            String operator = condition.contains("=") ? "=" : condition.contains("<") ? "<" : ">";
            String[] parts = condition.split(operator);
            String column = parts[0].trim();
            String value = parts[1].trim();

            int colIndex = columnIndices.get(column);
            String rowValue = rowData[colIndex];

            try {
                int rowInt = Integer.parseInt(rowValue);
                int condInt = Integer.parseInt(value);
                if ((operator.equals("=") && rowInt != condInt) ||
                    (operator.equals(">") && rowInt <= condInt) ||
                    (operator.equals("<") && rowInt >= condInt)) {
                    return false;
                }
            } catch (NumberFormatException e) {
                if (operator.equals("=") && !rowValue.equals(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static int applySetConditions(String[] setConditions, Map<String, Integer> columnIndices, String[] rowData,List<String[]> tableData) {
        for (String condition : setConditions) {
            String[] parts = condition.split("=");
            String column = parts[0].trim();
            String value = parts[1].trim();
            int colIndex = columnIndices.get(column);
            System.out.println(tableData.get(0)[colIndex]);
            if(tableData.get(0)[colIndex].toUpperCase().contains("NOT NULL")&& value.equalsIgnoreCase("null")){
            	System.out.println("Not Null Constraints Failed");
            	return -1;
            	
            }
            if(tableData.get(0)[colIndex].toUpperCase().contains("PRIMARY KEY") ||tableData.get(0)[colIndex].toUpperCase().contains("UNIQUE")) {
            	for(int k=1;k<tableData.size();k++) {
            		if(tableData.get(k)[colIndex].equals(value)) {
            			System.out.println("UNIQUE or PRIMARY KEY Constraint Failed");
            			return -1;
            		}
            	}
            }
            rowData[colIndex] = value;
        }
        return 1;
    }
}
