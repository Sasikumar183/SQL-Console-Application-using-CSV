package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class SelectTable {

    public static void select(String query) {
        query = query.substring(6).trim();
        int fromInd = query.toUpperCase().indexOf("FROM");
        int whereInd = query.toUpperCase().indexOf("WHERE");
        int limitInd = query.toUpperCase().indexOf("LIMIT");

        int limitValue = (limitInd == -1) ? -1 : Integer.parseInt(query.substring(limitInd + 6).trim());

        int firstKeywordInd = (whereInd == -1) ? limitInd : (limitInd == -1) ? whereInd : Math.min(whereInd, limitInd);
        String tableName = (firstKeywordInd == -1) ?
                query.substring(fromInd + 4).trim() :
                query.substring(fromInd + 4, firstKeywordInd).trim();

        File tableFile = new File(tableName + ".csv");
        if (!tableFile.exists()) {
            System.out.println("Table not found.");
            return;
        }

        List<String[]> tableData = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(tableFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                tableData.add(line.split(","));
            }
        } catch (IOException e) {
            System.out.println("Error reading table file: " + e.getMessage());
            return;
        }

        if (tableData.isEmpty()) {
            System.out.println("Table is empty.");
            return;
        }

        String[] columns = new String[tableData.get(0).length];
        for(int i=0;i<tableData.get(0).length;i++) {
        	columns[i]=tableData.get(0)[i].split(" ")[0];
        }
        	
        String columnPart = query.substring(0, fromInd).trim();
        String[] selectedColumns;

        if (columnPart.equals("*")) {
            selectedColumns = columns;
        } else {
            selectedColumns = columnPart.split(",");
            for (int i = 0; i < selectedColumns.length; i++) {
                selectedColumns[i] = selectedColumns[i].trim();
            }
        }

        String whereClause = whereInd == -1 ? null :
                (limitInd == -1 ? query.substring(whereInd + 5).trim() :
                        query.substring(whereInd + 5, limitInd).trim());

        List<String[]> validRows = new ArrayList<>();
        validRows.add(getSelectedColumns(columns, selectedColumns));

        for (int i = 1; i < tableData.size(); i++) {
            String[] rowData = tableData.get(i);
            boolean match = whereClause == null || evaluateWhereClause(columns, rowData, whereClause);

            if (match) {
                validRows.add(getSelectedRow(columns, rowData, selectedColumns));
            }
        }

        displayTable(validRows, limitValue);
    }

    private static boolean evaluateWhereClause(String[] columns, String[] rowData, String whereClause) {
        String[] conditions = whereClause.split(" ");
        boolean[] isOrCondition = new boolean[conditions.length - 1];

        List<String> conditionList = new ArrayList<>();
        int index = 0;
        StringBuilder currentCondition = new StringBuilder();

        for (String word : conditions) {
            if (word.equalsIgnoreCase("AND")) {
                conditionList.add(currentCondition.toString().trim());
                isOrCondition[index++] = false;
                currentCondition.setLength(0);
            } else if (word.equalsIgnoreCase("OR")) {
                conditionList.add(currentCondition.toString().trim());
                isOrCondition[index++] = true;
                currentCondition.setLength(0);
            } else {
                currentCondition.append(word).append(" ");
            }
        }
        conditionList.add(currentCondition.toString().trim());

        boolean match = false;
        boolean orMatch = false;
        for (int i = 0; i < conditionList.size(); i++) {
            String condition = conditionList.get(i);
            String[] parts = parseCondition(condition);
            if (parts.length != 2) continue;

            String whereColumn = parts[0].trim();
            String whereValue = parts[1].trim();
            int whereIndex = indexOfColumn(columns, whereColumn);

            boolean conditionResult = whereIndex != -1 && applyCondition(rowData[whereIndex], whereValue, condition);

            if (i == 0) {
                match = conditionResult;
            } else {
                if (isOrCondition[i - 1]) {
                    orMatch = orMatch || conditionResult;
                } else {
                    match = match && conditionResult;
                }
            }
        }
        return match || orMatch;
    }

    private static String[] parseCondition(String condition) {
        String op = condition.contains("=") ? "=" : (condition.contains(">") ? ">" : "<");
        return condition.split(op);
    }

    private static boolean applyCondition(String rowValue, String whereValue, String condition) {
        String op = condition.contains("=") ? "=" : (condition.contains(">") ? ">" : "<");

        try {
            int rowInt = Integer.parseInt(rowValue);
            int whereInt = Integer.parseInt(whereValue);

            return (op.equals("=") && rowInt == whereInt) ||
                    (op.equals(">") && rowInt > whereInt) ||
                    (op.equals("<") && rowInt < whereInt);
        } catch (NumberFormatException e) {
            return op.equals("=") && rowValue.equals(whereValue);
        }
    }

    private static int indexOfColumn(String[] columns, String columnName) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].trim().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private static String[] getSelectedColumns(String[] columns, String[] selectedColumns) {
        List<String> selectedCols = new ArrayList<>();
        for (String column : selectedColumns) {
            int columnIndex = indexOfColumn(columns, column);
            if (columnIndex != -1) {
                selectedCols.add(columns[columnIndex]);
            }
        }
        return selectedCols.toArray(new String[0]);
    }

    private static String[] getSelectedRow(String[] columns, String[] rowData, String[] selectedColumns) {
        List<String> selectedRow = new ArrayList<>();
        for (String column : selectedColumns) {
            int columnIndex = indexOfColumn(columns, column);
            if (columnIndex != -1 && columnIndex < rowData.length) { 
                selectedRow.add(rowData[columnIndex]);
            } else {
                selectedRow.add("NULL"); // Default value if column not found
            }
        }
        return selectedRow.toArray(new String[0]);
    }


    private static void displayTable(List<String[]> tableData, int limit) {
        int n = (limit == -1) ? tableData.size() : Math.min(limit + 1, tableData.size());
        for (int i = 0; i < n; i++) {
        	for(String s:tableData.get(i)) {
                System.out.printf("%-12s |",s);

        	}
            System.out.println();
        }
    }
}

