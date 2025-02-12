package com.example.sql_in_csv;

import java.io.*;
import java.util.*;
public class InsertData {
    public static void insert(String query) {
        query = query.substring(11).trim();
        int valInd = query.toUpperCase().indexOf("VALUES");
        if (valInd == -1) {
            System.out.println("Check the syntax");
            return;
        }

        String tablePart = query.substring(0, valInd).trim();
        int colStart = tablePart.indexOf('(');
        int colEnd = tablePart.indexOf(')');
        
        String tableName;
        String[] columns = null;

        if (colStart != -1 && colEnd != -1 && colStart < colEnd) {
            tableName = tablePart.substring(0, colStart).trim();
            columns = tablePart.substring(colStart + 1, colEnd).split(",");
        } else {
            tableName = tablePart;
        }

        int valStart = query.indexOf('(', valInd);
        int valEnd = query.indexOf(')', valInd);
        if (valStart == -1 || valEnd == -1 || valStart > valEnd) {
            System.out.println("Check the syntax");
            return;
        }

        String[] values = query.substring(valStart + 1, valEnd).split(",");
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].trim().replaceAll("'", "");
        }

        File tableFile = new File(tableName + ".csv");

        if (!tableFile.exists()) {
            System.out.println("Table does not exist");
            return;
        }

        List<String> existingRows = new ArrayList<>();
        List<String> headers;
       
        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            headers = Arrays.asList(br.readLine().split(","));
            String line;
            while ((line = br.readLine()) != null) {
                existingRows.add(line);
            }
        } catch (IOException e) {
            System.out.println("Error reading table file: " + e.getMessage());
            return;
        }


        if (columns != null && columns.length != values.length) {
            System.out.println("Column count does not match value count");
            return;
        }

        Map<String, String> colValueMap = new HashMap<>();
        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                String colName = columns[i].trim();
                boolean found = false;
                for (String header : headers) {
                    if (header.contains(colName)) {
                        colValueMap.put(colName, values[i]);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    System.out.println("Column '" + colName + "' not found in table");
                    return;
                }
            }
        } else {
            if (values.length != headers.size()) {
                System.out.println("Number of values does not match number of columns");
                return;
            }
            for (int i = 0; i < headers.size(); i++) {
                colValueMap.put(headers.get(i).split(" ")[0], values[i]);
            }
        }

        for (String header : headers) {
            String colName = header.split(" ")[0];
            String constraint = header;
            if (constraint != null) {
            	if (constraint.toUpperCase().contains("AUTO_INCREMENT")) {
                    int maxId = 0;
                    for (String row : existingRows) {
                        String[] rowData = row.split(",");
                        int colIndex = headers.indexOf(header);
                        if (colIndex < rowData.length) {
                            try {
                                int currentId = Integer.parseInt(rowData[colIndex]);
                                if (currentId > maxId) {
                                    maxId = currentId;
                                }
                            } catch (NumberFormatException ignored) {}
                        }
                    }
                    if (colValueMap.containsKey(colName) && !colValueMap.get(colName).equals("NULL")) {
                    	colValueMap.put(colName, colValueMap.get(colName));
                    } else {
                        colValueMap.put(colName, String.valueOf(maxId + 1));
                    }
                }
            	
                if (constraint.toUpperCase().contains("PRIMARY")) {
                    for (String row : existingRows) {
                        String[] rowData = row.split(",");
                        int colIndex = headers.indexOf(header);
                        if (colIndex < rowData.length && rowData[colIndex].equals(colValueMap.get(colName))) {
                            System.out.println("Error: Duplicate primary key value for column '" + colName + "'");
                            return;
                        }
                    }
                }

                if (constraint.toUpperCase().contains("UNIQUE")) {
                    for (String row : existingRows) {
                        String[] rowData = row.split(",");
                        int colIndex = headers.indexOf(header);
                        if (colIndex < rowData.length && rowData[colIndex].equals(colValueMap.get(colName))) {
                            System.out.println("Error: Duplicate unique value for column '" + colName + "'");
                            return;
                        }
                    }
                }

                if (constraint.toUpperCase().contains("NOT NULL")) {
                    if (colValueMap.get(colName) == null || colValueMap.get(colName).isEmpty()) {
                        System.out.println("Error: Column '" + colName + "' cannot be NULL");
                        return;
                    }
                }

                
            }
        }

        StringBuilder newRow = new StringBuilder();
        for (String header : headers) {
            newRow.append(colValueMap.getOrDefault(header.split(" ")[0], "NULL")).append(",");
        }
        //System.out.println(newRow);
        newRow.setLength(newRow.length() - 1);

        try (FileWriter writer = new FileWriter(tableFile, true)) {
            writer.write("\n"+newRow);
        } catch (IOException e) {
            System.out.println("Error writing to table file: " + e.getMessage());
            return;
        }

        System.out.println("Inserted into '" + tableName + "' successfully!");
    }
}
