package com.example.sql_in_csv;

import java.io.*;

public class Truncate {
    public static void truncate(String query) {
        String tableName = query.substring(14).trim();
        File file = new File(tableName + ".csv");

        if (!file.exists()) { 
            System.out.println("Table not found");
            return;
        }

        String header;
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            header = br.readLine(); 
            } catch (IOException e) {
            System.out.println("Error reading table: " + e.getMessage());
            return;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            if (header != null) {
                bw.write(header);
                bw.newLine();
            }
        } catch (IOException e) {
            System.out.println("Error writing table: " + e.getMessage());
        }

        System.out.println("Table '" + tableName + "' truncated successfully!");
    }
}
