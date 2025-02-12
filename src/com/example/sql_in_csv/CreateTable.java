package com.example.sql_in_csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateTable {
    public static void create(String query) {
        query = query.substring(12).trim();
        int stInd = query.indexOf('(');
        int endInd = query.lastIndexOf(')');

        if (stInd == -1 || endInd == -1 || stInd > endInd) {
            System.out.println("Check the syntax");
            return;
        }

        String tableName = query.substring(0, stInd).trim();
        File tableFile = new File(tableName + ".csv");

        if (tableFile.exists()) {
            System.out.println("Table already exists");
            return;
        }

        int i = query.toUpperCase().indexOf("PRIMARY KEY");
        int j = query.toUpperCase().lastIndexOf("PRIMARY KEY");
        if (i != j) {
            System.out.println("Multiple primary keys not allowed");
            return;
        }

        String[] columns = query.substring(stInd + 1, endInd).split(",");
        StringBuilder columnSchema = new StringBuilder();

        for (String column : columns) {
            String[] parts = column.trim().split("\\s+");
            if (parts.length < 2) {
                System.out.println("Syntax Error in column definition");
                return;
            }
            columnSchema.append(String.join(" ", parts)).append(",");
        }

        if (columnSchema.length() > 0) columnSchema.setLength(columnSchema.length() - 1);

        try (FileWriter tableWriter = new FileWriter(tableFile)) {
            tableWriter.write(columnSchema.toString());
        } catch (IOException e) {
            System.out.println("Error writing table file: " + e.getMessage());
        }

        System.out.println("Table '" + tableName + "' created successfully!");
    }
}
