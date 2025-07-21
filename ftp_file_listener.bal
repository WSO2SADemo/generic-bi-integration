import ballerina/ftp;
import ballerina/io;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerina/task;

// Track last processed line count for each file
map<int> lastProcessedLineCount = {};

// Track last file size to detect changes
map<int> lastFileSize = {};

// Polling task to check for file modifications (removed SFTP listener due to initialization issues)
task:JobId? pollingJobId = ();

// Initialize polling task when module loads
function init() {
    // Start polling task to check for file modifications every 30 seconds
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new FilePollingJob(), 30);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("SFTP file polling task started", interval = "30 seconds");
    } else {
        log:printError("Failed to start SFTP file polling task", 'error = result);
    }
}

// Job class for polling file changes
class FilePollingJob {
    
    public function execute() {
        // Check for modifications to data.csv
        error? checkResult = checkFileModifications("data.csv");
        if checkResult is error {
            log:printError("Error checking SFTP file modifications", 'error = checkResult);
        }
    }
}

// Function to check if file has been modified
function checkFileModifications(string fileName) returns error? {
    
    // Use the exact path from configuration
    string filePath = ftpFilePath; // "/telecomdemolocation/data.csv"
    log:printInfo("SFTP file polling started", filePath = filePath);
    
    // Get file content from SFTP server with error handling
    stream<byte[] & readonly, io:Error?>|ftp:Error fileStream = ftpClient->get(filePath);
    
    if fileStream is ftp:Error {
        log:printError("Failed to connect to SFTP server or file not found", 'error = fileStream);
        return fileStream;
    }
    
    log:printInfo("SFTP file accessed successfully");
    
    // Read file content
    byte[] fileContent = [];
    error? readResult = fileStream.forEach(function(byte[] & readonly chunk) {
        foreach byte b in chunk {
            fileContent.push(b);
        }
    });
    
    if readResult is error {
        log:printError("Failed to read SFTP file content", 'error = readResult);
        return readResult;
    }
    
    // Convert bytes to string
    string|error csvContent = string:fromBytes(fileContent);
    if csvContent is error {
        log:printError("Failed to convert file content to string", 'error = csvContent);
        return csvContent;
    }
    
    int currentFileSize = csvContent.length();
    
    // Check if file size has changed
    int previousFileSize = 0;
    if lastFileSize.hasKey(fileName) {
        int? storedSize = lastFileSize[fileName];
        if storedSize is int {
            previousFileSize = storedSize;
        }
    }
    
    if currentFileSize != previousFileSize {
        log:printInfo("SFTP file modification detected", 
            fileName = fileName,
            previousSize = previousFileSize,
            currentSize = currentFileSize
        );
        
        // Update file size tracking
        lastFileSize[fileName] = currentFileSize;
        
        // Process new lines
        error? processResult = processNewCSVLinesFromContent(fileName, csvContent);
        if processResult is error {
            log:printError("Failed to process modified CSV file from SFTP", 'error = processResult);
        }
    } else {
        log:printInfo("No changes detected in SFTP file", fileName = fileName, fileSize = currentFileSize);
    }
}

// Function to process new lines from CSV content
function processNewCSVLinesFromContent(string fileName, string csvContent) returns error? {
    
    // Split content into lines
    string[] lines = re `\r?\n`.split(csvContent);
    
    // Filter out empty lines
    string[] nonEmptyLines = [];
    foreach string line in lines {
        string trimmedLine = line.trim();
        if trimmedLine.length() > 0 {
            nonEmptyLines.push(trimmedLine);
        }
    }
    
    if nonEmptyLines.length() < 2 {
        return error("CSV file must contain at least header and one data row");
    }
    
    log:printInfo("SFTP file parsing details", 
        fileName = fileName,
        totalLines = lines.length(),
        nonEmptyLines = nonEmptyLines.length(),
        dataLines = nonEmptyLines.length() - 1
    );
    
    // Get last processed line count for this file (this tracks data lines, not including header)
    int lastProcessedDataLines = 0;
    if lastProcessedLineCount.hasKey(fileName) {
        int? storedCount = lastProcessedLineCount[fileName];
        if storedCount is int {
            lastProcessedDataLines = storedCount;
        }
    }
    
    log:printInfo("SFTP processing state", 
        fileName = fileName,
        lastProcessedDataLines = lastProcessedDataLines,
        currentDataLines = nonEmptyLines.length() - 1
    );
    
    int newLinesProcessed = 0;
    
    // Process only new data lines (skip header at index 0)
    // Start from (lastProcessedDataLines + 1) to get the next unprocessed line
    int startIndex = lastProcessedDataLines + 1; // +1 to account for header
    foreach int i in startIndex ..< nonEmptyLines.length() {
        string line = nonEmptyLines[i];
        error? lineResult = processCSVLine(line, i);
        if lineResult is error {
            log:printError("Failed to process CSV line from SFTP", lineNumber = i, 'error = lineResult);
        } else {
            newLinesProcessed += 1;
            log:printInfo("Processed SFTP line", lineNumber = i, content = line);
        }
    }
    
    // Update last processed data line count (excluding header)
    int currentDataLines = nonEmptyLines.length() - 1;
    lastProcessedLineCount[fileName] = currentDataLines;
    
    log:printInfo("Completed processing new CSV lines from SFTP", 
        fileName = fileName,
        newLinesProcessed = newLinesProcessed,
        totalDataLinesInFile = currentDataLines,
        lastProcessedDataLines = currentDataLines
    );
}

// Function to process individual CSV line
function processCSVLine(string csvLine, int lineNumber) returns error? {
    
    // Parse CSV line (assuming comma-separated values)
    string[] fields = re `,`.split(csvLine);
    
    if fields.length() != 6 {
        return error("Invalid CSV format. Expected 6 fields but found " + fields.length().toString());
    }
    
    // Create CustomerPlanData from CSV fields
    CustomerPlanData planData = {
        customerId: fields[0].trim(),
        phoneNumber: fields[1].trim(),
        planType: fields[2].trim(),
        monthlyCharge: check decimal:fromString(fields[3].trim()),
        status: fields[4].trim(),
        registrationDate: fields[5].trim()
    };
    
    // Create plan message
    PlanMessage planMessage = {
        messageId: uuid:createType1AsString(),
        timestamp: time:utcToString(time:utcNow()),
        planData: planData,
        eventType: "FILE_PLAN_UPDATE",
        sourceFile: ftpFilePath
    };
    
    // Publish to Kafka topic "telecom_plan"
    kafka:AnydataProducerRecord producerRecord = {
        topic: planTopicName,
        value: planMessage,
        key: planData.customerId
    };
    
    kafka:Error? result = kafkaProducer->send(producerRecord);
    if result is kafka:Error {
        return error("Failed to publish plan data to Kafka: " + result.message());
    }
    
    log:printInfo("New plan data published successfully from SFTP", 
        customerId = planData.customerId,
        phoneNumber = planData.phoneNumber,
        planType = planData.planType,
        lineNumber = lineNumber
    );
}

// Function to reset file position tracking (useful for testing or maintenance)
public function resetFilePosition(string fileName) {
    int? removedValue = lastProcessedLineCount.removeIfHasKey(fileName);
    int? removedSize = lastFileSize.removeIfHasKey(fileName);
    log:printInfo("Reset SFTP file position tracking", fileName = fileName);
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
            log:printError("Failed to stop SFTP file polling task", 'error = result);
        } else {
            log:printInfo("SFTP file polling task stopped successfully");
            pollingJobId = (); // Reset to null after successful unscheduling
        }
    } else {
        log:printWarn("No SFTP polling task to stop - task may not be running");
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
        log:printInfo("SFTP file polling task restarted", interval = "30 seconds");
    } else {
        log:printError("Failed to restart SFTP file polling task", 'error = result);
    }
}

// Function to test SFTP connection manually
public function testSFTPConnection() returns error? {
    log:printInfo("Testing SFTP connection", host = ftpHost, port = ftpPort, username = ftpUsername);
    
    stream<byte[] & readonly, io:Error?>|ftp:Error fileStream = ftpClient->get(ftpFilePath);
    
    if fileStream is ftp:Error {
        log:printError("SFTP connection test failed", 'error = fileStream);
        return fileStream;
    } else {
        log:printInfo("SFTP connection test successful");
        // Close the stream
        error? closeResult = fileStream.close();
        if closeResult is error {
            log:printWarn("Failed to close SFTP stream", 'error = closeResult);
        }
    }
}