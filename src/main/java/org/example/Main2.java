package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Main2 class for combining and processing multiple prospect CSV files with email deduplication.
 *
 * This application processes all CSV files from the 'prospects' folder on the user's desktop:
 * - Combines all records from multiple CSV files into one unified output
 * - Implements intelligent email handling: uses 'personal_email' as fallback if 'email' is empty
 * - Ensures email uniqueness by removing duplicate email addresses (case-insensitive)
 * - Skips records that have no email address in either 'email' or 'personal_email' columns
 * - Maintains the same column structure across all input files
 *
 * Output: 'combined_prospects.csv' containing all unique prospect records with valid emails.
 */
public class Main2 {

    /**
     * Main entry point for combining prospect CSV files with email processing and deduplication.
     * Processes all CSV files in the prospects folder and creates a unified, deduplicated output.
     */
    public static void main2(String[] args) {
        // Get the user's desktop path
        String desktopPath = System.getProperty("user.home") + "/Desktop";
        String prospectsFolder = desktopPath + "/prospects";
        String outputFile = desktopPath + "/combined_prospects.csv";

        try {
            combineProspectsCsvFiles(prospectsFolder, outputFile);
            System.out.println("Successfully combined all prospect CSV files into: " + outputFile);
        } catch (IOException e) {
            System.err.println("Error processing prospect files: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Combines all CSV files from the prospects folder into a single output file with email processing.
     *
     * Features:
     * - Reads all .csv files from the specified folder
     * - Uses first file's headers as the master column structure
     * - Processes email fields with intelligent fallback (email -> personal_email)
     * - Removes duplicate email addresses (case-insensitive comparison)
     * - Skips records with no valid email address
     * - Provides detailed processing statistics
     *
     * @param prospectsFolder Path to the folder containing prospect CSV files
     * @param outputFile Path for the combined output CSV file
     * @throws IOException if file processing fails
     */
    private static void combineProspectsCsvFiles(String prospectsFolder, String outputFile) throws IOException {
        Path prospectsPath = Paths.get(prospectsFolder);

        if (!Files.exists(prospectsPath) || !Files.isDirectory(prospectsPath)) {
            throw new IOException("Prospects folder does not exist: " + prospectsFolder);
        }

        List<String> headers = null;
        boolean isFirstFile = true;
        int totalRecords = 0;
        int processedFiles = 0;
        int skippedRecords = 0;
        int duplicateRecords = 0;

        // Track unique emails to avoid duplicates
        Set<String> uniqueEmails = new HashSet<>();

        try (Writer writer = Files.newBufferedWriter(Paths.get(outputFile))) {
            CSVPrinter csvPrinter = null;

            // Read all CSV files from the prospects folder
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(prospectsPath, "*.csv")) {
                for (Path csvFile : stream) {
                    System.out.println("Processing file: " + csvFile.getFileName());

                    try (Reader reader = Files.newBufferedReader(csvFile);
                         CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                                 .withFirstRecordAsHeader()
                                 .withIgnoreEmptyLines(true)
                                 .withTrim(true)
                                 .withAllowMissingColumnNames(true))) {

                        if (isFirstFile) {
                            // Get headers from the first file and create CSV printer
                            headers = new ArrayList<>(csvParser.getHeaderNames());
                            csvPrinter = new CSVPrinter(writer,
                                CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])));
                            isFirstFile = false;
                            System.out.println("Headers found: " + String.join(", ", headers));
                        } else {
                            // Validate that current file has the same headers
                            List<String> currentHeaders = new ArrayList<>(csvParser.getHeaderNames());
                            if (!headers.equals(currentHeaders)) {
                                System.out.println("Warning: File " + csvFile.getFileName() +
                                    " has different headers. Expected: " + headers +
                                    ", Found: " + currentHeaders);
                                // Continue processing but log the warning
                            }
                        }

                        // Copy records from current file with email processing and deduplication
                        int fileRecords = 0;
                        int fileSkipped = 0;
                        int fileDuplicates = 0;
                        for (CSVRecord record : csvParser) {
                            List<String> recordValues = new ArrayList<>();

                            // Check email and personal_email values
                            String emailValue = "";
                            String personalEmailValue = "";

                            try {
                                emailValue = record.get("email");
                                if (emailValue == null) emailValue = "";
                            } catch (IllegalArgumentException e) {
                                emailValue = "";
                            }

                            try {
                                personalEmailValue = record.get("personal_email");
                                if (personalEmailValue == null) personalEmailValue = "";
                            } catch (IllegalArgumentException e) {
                                personalEmailValue = "";
                            }

                            // Skip if both email and personal_email are empty
                            if (emailValue.trim().isEmpty() && personalEmailValue.trim().isEmpty()) {
                                fileSkipped++;
                                skippedRecords++;
                                continue;
                            }

                            // Use personal_email if email is empty but personal_email is not
                            String finalEmailValue = emailValue.trim().isEmpty() ? personalEmailValue : emailValue;

                            // Check for duplicate email and skip if already processed
                            String normalizedEmail = finalEmailValue.trim().toLowerCase();
                            if (uniqueEmails.contains(normalizedEmail)) {
                                fileDuplicates++;
                                duplicateRecords++;
                                continue;
                            }

                            // Add email to unique set
                            uniqueEmails.add(normalizedEmail);

                            // Extract values for each header column
                            for (int i = 0; i < headers.size(); i++) {
                                String header = headers.get(i);
                                String value = "";

                                if (header.equalsIgnoreCase("email")) {
                                    // Use the processed email value
                                    value = finalEmailValue;
                                } else {
                                    try {
                                        value = record.get(header);
                                        if (value == null) {
                                            value = "";
                                        }
                                    } catch (IllegalArgumentException e) {
                                        // Column doesn't exist in this file, use empty string
                                        value = "";
                                    }
                                }
                                recordValues.add(value);
                            }

                            csvPrinter.printRecord(recordValues);
                            fileRecords++;
                            totalRecords++;
                        }

                        System.out.println("  - Records from " + csvFile.getFileName() + ": " + fileRecords);
                        if (fileSkipped > 0) {
                            System.out.println("  - Skipped records (no email): " + fileSkipped);
                        }
                        if (fileDuplicates > 0) {
                            System.out.println("  - Duplicate emails skipped: " + fileDuplicates);
                        }
                        processedFiles++;

                    } catch (IOException e) {
                        System.err.println("Error reading file " + csvFile.getFileName() + ": " + e.getMessage());
                        // Continue with other files
                    }
                }
            }

            if (csvPrinter != null) {
                csvPrinter.flush();
            }

            System.out.println("\n=== Summary ===");
            System.out.println("Files processed: " + processedFiles);
            System.out.println("Total records combined: " + totalRecords);
            System.out.println("Total records skipped (no email): " + skippedRecords);
            System.out.println("Total duplicate emails skipped: " + duplicateRecords);
            System.out.println("Unique emails in output: " + uniqueEmails.size());
            System.out.println("Output file: " + outputFile);

            if (processedFiles == 0) {
                System.out.println("No CSV files found in the prospects folder!");
            }
        }
    }

    /**
     * Alternative main method for running this class directly.
     * Simply delegates to the main2 method for backward compatibility.
     */
    public static void main(String[] args) {
        main2(args);
    }
}
