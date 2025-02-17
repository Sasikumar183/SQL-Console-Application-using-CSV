package com.example.sql_in_csv;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DeleteData {
    public static void delete(String query) {
        query = query.substring(12).trim();
        int whereInd = query.toUpperCase().indexOf("WHERE");

        if (whereInd == -1) {
            System.out.println("Missing WHERE clause");
            return;
        }

        String tableName = query.substring(0, whereInd).trim();
        String whereClause = query.substring(whereInd + 5).trim();
        String[] conditions = whereClause.split("AND");

        File tableFile = new File(tableName + ".csv");
        if (!tableFile.exists()) {
            System.out.println("Table Not Found");
            return;
        }

        List<String[]> rows = new ArrayList<>();
        String[] columns = null;
        int deletedCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String line;
            boolean firstRow = true;

            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");

                if (firstRow) {
                    columns = row;
                    rows.add(row);
                    firstRow = false;
                    continue;
                }

                boolean match = true;

                for (String condition : conditions) {
                    condition = condition.trim();
                    String op = condition.contains("=") ? "=" :
                                condition.contains(">") ? ">" :
                                condition.contains("<") ? "<" : null;

                    if (op == null) {
                        System.out.println("Invalid WHERE conditions.");
                        return;
                    }

                    String[] parts = condition.split(op);
                    if (parts.length != 2) {
                        System.out.println("Invalid condition format. Must be column=value");
                        return;
                    }

                    String whereCol = parts[0].trim();
                    String whereVal = parts[1].trim();

                    int whereIndx = -1;
                    for (int c = 0; c < columns.length; c++) {
                        if (columns[c].split(" ")[0].equals(whereCol)) {
                            whereIndx = c;
                            break;
                        }
                    }

                    if (whereIndx == -1) {
                        System.out.println("Column " + whereCol + " not found.");
                        return;
                    }

                    String rowValue = row[whereIndx];

                    try {
                        int rowInt = Integer.parseInt(rowValue);
                        int condInt = Integer.parseInt(whereVal);

                        if ((op.equals("=") && rowInt != condInt) ||
                            (op.equals(">") && rowInt <= condInt) ||
                            (op.equals("<") && rowInt >= condInt)) {
                            match = false;
                            break;
                        }
                    } catch (NumberFormatException e) {
                        if (op.equals("=") && !rowValue.equals(whereVal)) {
                            match = false;
                            break;
                        }
                    }
                }

                if (match) {
                    deletedCount++;
                } else {
                    rows.add(row);
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading table: " + e.getMessage());
            return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(tableFile))) {
        	for (int i = 0; i < rows.size(); i++) {
        	    bw.write(String.join(",", rows.get(i)));
        	    if (i < rows.size() - 1) {
        	        bw.newLine();
        	    }
        	}
        } catch (IOException e) {
            System.out.println("Error writing table: " + e.getMessage());
        }

        System.out.println(deletedCount > 0 ? deletedCount + " Rows Deleted" : "No Rows Deleted");
    }
}
