package com.example.sql_in_csv;

import java.io.*;
import java.util.*;

public class FullJoin {

    public static void join(String query) {
        int from = query.toUpperCase().indexOf("FROM");
        int join = query.toUpperCase().indexOf("FULL JOIN");
        int on = query.toUpperCase().indexOf("ON");

        String leftTable = query.substring(from + 5, join).trim();
        String rightTable = query.substring(join + 10, on).trim();
        String condition = query.substring(on + 2).trim();

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

        File leftFile = new File(leftTable + ".csv");
        File rightFile = new File(rightTable + ".csv");

        if (!leftFile.exists() || !rightFile.exists()) {
            System.out.println("One or both tables not found.");
            return;
        }

        List<List<String>> leftTableData = InnerJoin.readCSV(leftTable + ".csv");
        List<List<String>> rightTableData = InnerJoin.readCSV(rightTable + ".csv");

        String leftJoinColumn = conditionParts[0].trim();
        String rightJoinColumn = conditionParts[1].trim();

        if (leftJoinColumn.contains(".")) leftJoinColumn = leftJoinColumn.split("\\.")[1];
        if (rightJoinColumn.contains(".")) rightJoinColumn = rightJoinColumn.split("\\.")[1];

        List<List<String>> result = performFullJoin(leftTableData, rightTableData, leftJoinColumn, rightJoinColumn, leftCol, rightCol);

        System.out.println("\nFull Join Result:");
        InnerJoin.displayTableInner(result,order);
    }

    public static List<List<String>> performFullJoin(List<List<String>> leftTableData, List<List<String>> rightTableData,
                                                     String leftJoinColumn, String rightJoinColumn,
                                                     List<String> leftCol, List<String> rightCol) {
    	
        List<List<String>> result = new ArrayList<>();
        int leftIndex = leftTableData.get(0).indexOf(leftJoinColumn);
        int rightIndex = rightTableData.get(0).indexOf(rightJoinColumn);

        if (leftIndex == -1 || rightIndex == -1) {
            System.out.println("Error: Column not found in one of the tables.");
            return result;
        }

        List<String> headerRow = new ArrayList<>();
        for (String col : leftCol) headerRow.add(leftTableData.get(0).get(leftTableData.get(0).indexOf(col)));
        for (String col : rightCol) headerRow.add(rightTableData.get(0).get(rightTableData.get(0).indexOf(col)));
        result.add(headerRow);

        Set<Integer> matchedRightRows = new HashSet<>();

        for (int i = 1; i < leftTableData.size(); i++) {
            boolean found = false;
            for (int j = 1; j < rightTableData.size(); j++) {
                if (leftTableData.get(i).get(leftIndex).equals(rightTableData.get(j).get(rightIndex))) {
                    List<String> joinedRow = new ArrayList<>();
                    found = true;
                    matchedRightRows.add(j);
                    for (String col : leftCol) joinedRow.add(leftTableData.get(i).get(leftTableData.get(0).indexOf(col)));
                    for (String col : rightCol) joinedRow.add(rightTableData.get(j).get(rightTableData.get(0).indexOf(col)));
                    result.add(joinedRow);
                }
            }
            if (!found) {
                List<String> joinedRow = new ArrayList<>();
                for (String col : leftCol) joinedRow.add(leftTableData.get(i).get(leftTableData.get(0).indexOf(col)));
                for (int k = 0; k < rightCol.size(); k++) joinedRow.add("NULL");
                result.add(joinedRow);
            }
        }

        for (int j = 1; j < rightTableData.size(); j++) {
            if (!matchedRightRows.contains(j)) {
                List<String> joinedRow = new ArrayList<>();
                for (int k = 0; k < leftCol.size(); k++) joinedRow.add("NULL");
                for (String col : rightCol) joinedRow.add(rightTableData.get(j).get(rightTableData.get(0).indexOf(col)));
                result.add(joinedRow);
            }
        }

        return result;
    }
}
