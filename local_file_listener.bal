import ballerina/io;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerina/task;
import ballerina/file;

// Track last processed line count for each file
map<int> lastProcessedLineCount = {};

// Track last file modification time to detect changes
map<int> lastFileModTime = {};

// Flag to track if initial processing is done
boolean initialProcessingDone = false;

// Polling task to check for file modifications
task:JobId? pollingJobId = ();

// Initialize polling task when module loads
function init() {
    // Start polling task to check for file modifications every 30 seconds
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new FilePollingJob(), 5);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("Local file polling task started", interval = "5 seconds");
    } else {
        log:printError("Failed to start local file polling task", 'error = result);
    }
}

// Job class for polling file changes
class FilePollingJob {
    
    public function execute() {
        // Only start polling after initial processing is complete
        if !initialProcessingDone {
            log:printInfo("Skipping polling - initial processing not yet complete");
            return;
        }
        
        // Check for modifications to data.csv
        error? checkResult = checkFileModifications("data.csv");
        if checkResult is error {
            log:printError("Error checking local file modifications", 'error = checkResult);
        }
    }
}

// Function to check if file has been modified
function checkFileModifications(string fileName) returns error? {
    
    log:printInfo("Local file polling started", filePath = localFilePath);
    
    // Check if file exists
    boolean|file:Error fileExists = file:test(localFilePath, file:EXISTS);
    if fileExists is file:Error {
        return error("Error checking file existence: " + fileExists.message());
    }
    if !fileExists {
        return error("File does not exist: " + localFilePath);
    }
    
    // Use current timestamp as a simple modification check
    // In a real scenario, you would check actual file modification time
    int currentTime = <int>time:utcNow()[0];
    
    // Check if enough time has passed since last check (to simulate file modification)
    int lastModTime = 0;
    if lastFileModTime.hasKey(fileName) {
        int? storedTime = lastFileModTime[fileName];
        if storedTime is int {
            lastModTime = storedTime;
        }
    }
    
    // For demonstration, we'll check if the file content has actually changed
    // by reading it and comparing with what we've processed
    string[][]|io:Error csvData = io:fileReadCsv(path = localFilePath);
    if csvData is io:Error {
        return error("Failed to read CSV file for modification check: " + csvData.message());
    }
    
    int currentDataLines = csvData.length() - 1; // Exclude header
    int lastProcessedDataLines = 0;
    if lastProcessedLineCount.hasKey(fileName) {
        int? storedCount = lastProcessedLineCount[fileName];
        if storedCount is int {
            lastProcessedDataLines = storedCount;
        }
    }
    
    if currentDataLines > lastProcessedDataLines {
        log:printInfo("New data detected in local file", 
            fileName = fileName,
            lastProcessedLines = lastProcessedDataLines,
            currentDataLines = currentDataLines
        );
        
        // Update modification time tracking
        lastFileModTime[fileName] = currentTime;
        
        // Process new lines
        error? processResult = processNewCSVLines(fileName);
        if processResult is error {
            log:printError("Failed to process modified CSV file", 'error = processResult);
            return processResult;
        }
    } else {
        log:printInfo("No new data detected in local file", 
            fileName = fileName, 
            processedLines = lastProcessedDataLines,
            currentLines = currentDataLines
        );
    }
}

// Function to process new lines from CSV file
function processNewCSVLines(string fileName) returns error? {
    
    log:printInfo("Starting to process new CSV lines", fileName = fileName);
    
    // Read CSV file from local file system using io:fileReadCsv
    string[][]|io:Error csvData = io:fileReadCsv(path = localFilePath);
    
    if csvData is io:Error {
        log:printError("Failed to read CSV file", 'error = csvData);
        return error("Failed to read CSV file: " + csvData.message());
    }
    
    if csvData.length() < 2 {
        log:printError("CSV file validation failed", totalRows = csvData.length());
        return error("CSV file must contain at least header and one data row");
    }
    
    log:printInfo("Local file parsing details", 
        fileName = fileName,
        totalRows = csvData.length(),
        dataRows = csvData.length() - 1
    );
    
    // Print the header for debugging
    if csvData.length() > 0 {
        log:printInfo("CSV Header", header = csvData[0].toString());
    }
    
    // Get last processed line count for this file (this tracks data lines, not including header)
    int lastProcessedDataLines = 0;
    if lastProcessedLineCount.hasKey(fileName) {
        int? storedCount = lastProcessedLineCount[fileName];
        if storedCount is int {
            lastProcessedDataLines = storedCount;
        }
    }
    
    log:printInfo("Local file processing state", 
        fileName = fileName,
        lastProcessedDataLines = lastProcessedDataLines,
        currentDataLines = csvData.length() - 1
    );
    
    int newLinesProcessed = 0;
    
    // Process only new data lines (skip header at index 0)
    // Start from (lastProcessedDataLines + 1) to get the next unprocessed line
    int startIndex = lastProcessedDataLines + 1; // +1 to account for header
    
    log:printInfo("Processing rows from index", startIndex = startIndex, totalRows = csvData.length());
    
    // Process each row individually with detailed error handling
    foreach int i in startIndex ..< csvData.length() {
        log:printInfo("About to process row", rowIndex = i);
        
        if i >= csvData.length() {
            log:printError("Row index out of bounds", rowIndex = i, totalRows = csvData.length());
            continue;
        }
        
        string[] row = csvData[i];
        log:printInfo("Processing row", rowIndex = i, rowData = row.toString());
        
        error? lineResult = processCSVRow(row, i);
        if lineResult is error {
            log:printError("Failed to process CSV row", lineNumber = i, 'error = lineResult);
            // Continue processing other rows even if one fails
        } else {
            newLinesProcessed += 1;
            log:printInfo("Successfully processed local file row", lineNumber = i, content = row.toString());
        }
    }
    
    // Update last processed data line count (excluding header)
    int currentDataLines = csvData.length() - 1;
    lastProcessedLineCount[fileName] = currentDataLines;
    
    log:printInfo("Completed processing new CSV lines from local file", 
        fileName = fileName,
        newLinesProcessed = newLinesProcessed,
        totalDataLinesInFile = currentDataLines,
        lastProcessedDataLines = currentDataLines
    );
    
    return ();
}

// Function to process individual CSV row
function processCSVRow(string[] fields, int lineNumber) returns error? {
    
    log:printInfo("Processing CSV row fields", lineNumber = lineNumber, fieldCount = fields.length(), fields = fields.toString());
    
    if fields.length() != 6 {
        string errorMsg = "Invalid CSV format. Expected 6 fields but found " + fields.length().toString();
        log:printError(errorMsg, lineNumber = lineNumber, actualFields = fields.toString());
        return error(errorMsg);
    }
    
    // Validate and trim fields
    string customerId = fields[0].trim();
    string phoneNumber = fields[1].trim();
    string planType = fields[2].trim();
    string monthlyChargeStr = fields[3].trim();
    string status = fields[4].trim();
    string registrationDate = fields[5].trim();
    
    log:printInfo("Parsed CSV fields", 
        customerId = customerId,
        phoneNumber = phoneNumber,
        planType = planType,
        monthlyChargeStr = monthlyChargeStr,
        status = status,
        registrationDate = registrationDate
    );
    
    // Parse monthly charge with error handling
    decimal|error monthlyChargeResult = decimal:fromString(monthlyChargeStr);
    if monthlyChargeResult is error {
        string errorMsg = "Failed to parse monthly charge: " + monthlyChargeResult.message();
        log:printError(errorMsg, lineNumber = lineNumber, monthlyChargeStr = monthlyChargeStr);
        return error(errorMsg);
    }
    
    log:printInfo("Successfully parsed monthly charge", monthlyCharge = monthlyChargeResult);
    
    // Use the types from types.bal
    CustomerPlanData planData = {
        customerId: customerId,
        phoneNumber: phoneNumber,
        planType: planType,
        monthlyCharge: monthlyChargeResult,
        status: status,
        registrationDate: registrationDate
    };
    
    log:printInfo("Created plan data", planData = planData.toString());
    
    // Generate message ID and timestamp
    string messageId = uuid:createType1AsString();
    string timestamp = time:utcToString(time:utcNow());
    
    log:printInfo("Generated message metadata", messageId = messageId, timestamp = timestamp);
    
    // Create plan message using the type from types.bal
    PlanMessage planMessage = {
        messageId: messageId,
        timestamp: timestamp,
        planData: planData,
        eventType: "FILE_PLAN_UPDATE",
        sourceFile: localFilePath
    };
    
    log:printInfo("Created plan message", messageId = planMessage.messageId, eventType = planMessage.eventType);
    
    // Publish to Kafka topic "telecom_plan"
    kafka:AnydataProducerRecord producerRecord = {
        topic: planTopicName,
        value: planMessage,
        key: planData.customerId
    };
    
    log:printInfo("Publishing to Kafka", topic = planTopicName, key = planData.customerId);
    
    kafka:Error? result = kafkaProducer->send(producerRecord);
    if result is kafka:Error {
        string errorMsg = "Failed to publish plan data to Kafka: " + result.message();
        log:printError(errorMsg, 'error = result);
        return error(errorMsg);
    }
    
    log:printInfo("New plan data published successfully from local file", 
        customerId = planData.customerId,
        phoneNumber = planData.phoneNumber,
        planType = planData.planType,
        lineNumber = lineNumber
    );
    
    return ();
}

// Function to reset file position tracking (useful for testing or maintenance)
public function resetFilePosition(string fileName) {
    int? removedValue = lastProcessedLineCount.removeIfHasKey(fileName);
    int? removedTime = lastFileModTime.removeIfHasKey(fileName);
    log:printInfo("Reset local file position tracking", fileName = fileName);
}

// Function to get current file position (useful for monitoring)
public function getFilePosition(string fileName) returns int {
    if lastProcessedLineCount.hasKey(fileName) {
        int? position = lastProcessedLineCount[fileName];
        if position is int {
            return position;
        }
    }
    return 0;
}

// Function to stop polling (useful for cleanup)
public function stopFilePolling() {
    task:JobId? currentJobId = pollingJobId;
    if currentJobId is task:JobId {
        task:Error? result = task:unscheduleJob(currentJobId);
        if result is task:Error {
            log:printError("Failed to stop local file polling task", 'error = result);
        } else {
            log:printInfo("Local file polling task stopped successfully");
            pollingJobId = (); // Reset to null after successful unscheduling
        }
    } else {
        log:printWarn("No local file polling task to stop - task may not be running");
    }
}

// Function to restart polling (useful for maintenance)
public function restartFilePolling() {
    // Stop existing polling if running
    stopFilePolling();
    
    // Start new polling task
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new FilePollingJob(), 30);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("Local file polling task restarted", interval = "30 seconds");
    } else {
        log:printError("Failed to restart local file polling task", 'error = result);
    }
}

// Function to test local file access manually
public function testLocalFileAccess() returns error? {
    log:printInfo("Testing local file access", filePath = localFilePath);
    
    // Check if file exists
    boolean|file:Error fileExists = file:test(localFilePath, file:EXISTS);
    if fileExists is file:Error {
        log:printError("Local file access test failed", 'error = fileExists);
        return fileExists;
    }
    if !fileExists {
        error fileNotFoundError = error("Local file not found: " + localFilePath);
        log:printError("Local file access test failed", 'error = fileNotFoundError);
        return fileNotFoundError;
    }
    
    // Try to read the file using io:fileReadCsv
    string[][]|io:Error csvData = io:fileReadCsv(path = localFilePath);
    if csvData is io:Error {
        log:printError("Local file access test failed", 'error = csvData);
        return csvData;
    } else {
        log:printInfo("Local file access test successful", rowCount = csvData.length());
        
        // Print all rows for debugging
        foreach int i in 0 ..< csvData.length() {
            log:printInfo("CSV Row", index = i, data = csvData[i].toString());
        }
    }
    
    return ();
}

// Function to test Kafka connectivity
public function testKafkaConnection() returns error? {
    log:printInfo("Testing Kafka connectivity");
    
    // Create a simple test message
    record {
        string testId;
        string timestamp;
        string message;
    } testMessage = {
        testId: uuid:createType1AsString(),
        timestamp: time:utcToString(time:utcNow()),
        message: "Kafka connectivity test"
    };
    
    // Try to send a test message
    kafka:AnydataProducerRecord testRecord = {
        topic: planTopicName,
        value: testMessage,
        key: "test-key"
    };
    
    kafka:Error? result = kafkaProducer->send(testRecord);
    if result is kafka:Error {
        log:printError("Kafka connectivity test failed", 'error = result);
        return result;
    }
    
    log:printInfo("Kafka connectivity test message sent successfully");
    return ();
}

// Function to process all rows in the file (for initial setup)
public function processAllRows() returns error? {
    log:printInfo("Processing all rows in the file");
    
    // Reset file position to process all rows
    resetFilePosition("data.csv");
    
    // Process the file
    error? result = processNewCSVLines("data.csv");
    if result is error {
        log:printError("Failed to process all rows", 'error = result);
        return result;
    }
    
    // Mark initial processing as done
    initialProcessingDone = true;
    log:printInfo("Initial processing completed - polling will now handle future changes");
    
    return ();
}

// Function to simulate file modification for testing
public function simulateFileModification() returns error? {
    log:printInfo("Simulating file modification for testing");
    
    // Reset the last processed count to simulate new data
    lastProcessedLineCount["data.csv"] = 0;
    
    // Process the file again
    error? result = processNewCSVLines("data.csv");
    if result is error {
        log:printError("Failed to simulate file modification", 'error = result);
        return result;
    }
    
    log:printInfo("File modification simulation completed");
    return ();
}

// Function to add new data to file and process (for testing)
public function addNewDataAndProcess(string[] newRow) returns error? {
    log:printInfo("Adding new data and processing", newRowData = newRow.toString());
    
    // In a real scenario, you would append to the actual file
    // For now, we'll just simulate processing the new row
    error? result = processCSVRow(newRow, 999); // Using 999 as a test line number
    if result is error {
        log:printError("Failed to process new data", 'error = result);
        return result;
    }
    
    log:printInfo("New data processed successfully");
    return ();
}