package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Main class for filtering scraped CSV data based on verified email addresses.
 *
 * This application processes two CSV files from the user's desktop:
 * 1. 'checked.csv' - Contains verified email addresses (emails from second column)
 * 2. 'scraped.csv' - Contains scraped data with email information
 *
 * The program filters the scraped data to only include records where the email
 * address is present in the checked file, and removes specified unwanted columns
 * (Followers, Following, Tweets, Profile picture link, Screen name, Bio).
 *
 * Output: 'filtered_scraped.csv' containing only verified email records with cleaned columns.
 */
public class Main {

    /**
     * Main entry point that orchestrates the email verification and filtering process.
     * Reads verified emails from 'checked.csv', filters 'scraped.csv' based on those emails,
     * removes unwanted columns, and outputs the result to 'filtered_scraped.csv'.
     */
    public static void main(String[] args) {
        // Get the user's desktop path
        String desktopPath = System.getProperty("user.home") + "/Desktop";
        String checkedFile = desktopPath + "/checked.csv";
        String scrapedFile = desktopPath + "/scraped.csv";
        String outputFile = desktopPath + "/filtered_scraped.csv";

        // Columns to remove from scraped CSV (based on actual column names)
        Set<String> columnsToRemove = Set.of(
            "Followers", "Following", "Tweets",
            "Profile picture link", "Screen name", "Bio"
        );

        try {
            // Step 1: Read checked emails
            Set<String> checkedEmails = readCheckedEmails(checkedFile);
            System.out.println("Loaded " + checkedEmails.size() + " checked emails");

            // Step 2: Process scraped file
            filterScrapedFile(scrapedFile, outputFile, checkedEmails, columnsToRemove);

            System.out.println("Processing completed! Filtered file saved as: " + outputFile);

        } catch (IOException e) {
            System.err.println("Error processing files: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Reads verified email addresses from the checked CSV file.
     * Extracts emails from the second column (index 1) of the CSV file,
     * normalizes them to lowercase, and filters out invalid entries.
     *
     * @param filePath Path to the checked.csv file
     * @return Set of verified email addresses (normalized to lowercase)
     * @throws IOException if file reading fails
     */
    private static Set<String> readCheckedEmails(String filePath) throws IOException {
        Set<String> emails = new HashSet<>();

        try (Reader reader = Files.newBufferedReader(Paths.get(filePath));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withIgnoreEmptyLines(true)
                     .withTrim(true)
                     .withAllowMissingColumnNames(true))) {

            for (CSVRecord record : csvParser) {
                // Read email from second column (index 1)
                if (record.size() > 1) {
                    String email = record.get(1);
                    if (email != null && !email.trim().isEmpty() &&
                        !email.trim().equalsIgnoreCase("ok") &&
                        !email.trim().equalsIgnoreCase("ELV Result")) {
                        emails.add(email.trim().toLowerCase());
                    }
                }
            }
        }

        return emails;
    }

    /**
     * Filters the scraped CSV file based on verified emails and removes unwanted columns.
     * Only includes records where the email field matches an email from the checked file.
     * Removes specified columns (Followers, Following, Tweets, etc.) from the output.
     *
     * @param inputFile Path to the scraped.csv file
     * @param outputFile Path for the filtered output file
     * @param checkedEmails Set of verified email addresses
     * @param columnsToRemove Set of column names to exclude from output
     * @throws IOException if file processing fails
     */
    private static void filterScrapedFile(String inputFile, String outputFile,
                                        Set<String> checkedEmails, Set<String> columnsToRemove) throws IOException {

        try (Reader reader = Files.newBufferedReader(Paths.get(inputFile));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                     .withFirstRecordAsHeader()
                     .withIgnoreEmptyLines(true)
                     .withTrim(true)
                     .withAllowMissingColumnNames(true));
             Writer writer = Files.newBufferedWriter(Paths.get(outputFile))) {

            // Get headers and filter out unwanted columns
            List<String> originalHeaders = csvParser.getHeaderNames();
            List<String> filteredHeaders = new ArrayList<>();

            for (String header : originalHeaders) {
                if (header != null && !header.trim().isEmpty() && !columnsToRemove.contains(header)) {
                    filteredHeaders.add(header);
                }
            }

            // Find email column index for filtered headers
            String emailColumn = findEmailColumn(originalHeaders);

            // Create CSV printer with filtered headers
            CSVPrinter csvPrinter = new CSVPrinter(writer,
                CSVFormat.DEFAULT.withHeader(filteredHeaders.toArray(new String[0])));

            int totalRows = 0;
            int filteredRows = 0;

            // Read scraped file line by line
            for (CSVRecord record : csvParser) {
                totalRows++;

                // Get email from current line
                String email = record.get(emailColumn);

                // Check if email is present and not empty
                if (email != null && !email.trim().isEmpty()) {
                    String normalizedEmail = email.trim().toLowerCase();

                    // Only save line if email is present in checked file
                    if (checkedEmails.contains(normalizedEmail)) {
                        // Create filtered record (omit unwanted columns)
                        List<String> filteredRecord = new ArrayList<>();
                        for (String header : filteredHeaders) {
                            String value = record.get(header);
                            filteredRecord.add(value != null ? value : "");
                        }

                        // Save the line to result CSV
                        csvPrinter.printRecord(filteredRecord);
                        filteredRows++;
                    }
                }
            }

            csvPrinter.flush();

            System.out.println("Total rows processed: " + totalRows);
            System.out.println("Rows with verified emails saved: " + filteredRows);
            System.out.println("Removed columns: " + String.join(", ", columnsToRemove));
        }
    }

    /**
     * Finds the email column in the CSV headers by searching for common email column names.
     * Tries variations like "email", "Email", "EMAIL", "e-mail", etc.
     *
     * @param headers List of CSV header names
     * @return Name of the email column
     * @throws RuntimeException if no email column is found
     */
    private static String findEmailColumn(List<String> headers) {
        // Try to find email column by common names
        String[] emailColumnNames = {"email", "Email", "EMAIL", "e-mail", "E-mail", "mail", "Mail"};

        for (String possibleName : emailColumnNames) {
            if (headers.contains(possibleName)) {
                return possibleName;
            }
        }

        // If no standard email column found, look for columns containing "email"
        for (String header : headers) {
            if (header.toLowerCase().contains("email")) {
                return header;
            }
        }

        throw new RuntimeException("Could not find emails column in CSV. Available columns: " +
                                 String.join(", ", headers));
    }
}