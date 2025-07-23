import ballerina/file;
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

// Polling task to check for file modifications
task:JobId? pollingJobId = ();

// Initialize polling task when module loads
function init() {
    // Start polling task to check for file modifications every 30 seconds
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new LocalFilePollingJob(), 5);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("Local file polling task started", interval = "5 seconds");
    } else {
        log:printError("Failed to start local file polling task", 'error = result);
    }
}

// Job class for polling local file changes
class LocalFilePollingJob {
    
    public function execute() {
        // Check for modifications to the local CSV file
        error? checkResult = checkLocalFileModifications();
        if checkResult is error {
            log:printError("Error checking local file modifications", 'error = checkResult);
        }
    }
}

// Function to check if local file has been modified
function checkLocalFileModifications() returns error? {
    
    log:printInfo("Local file polling started", filePath = localFilePath);
    
    // Check if file exists
    boolean fileExists = check file:test(localFilePath, file:EXISTS);
    if !fileExists {
        log:printWarn("Local CSV file does not exist", filePath = localFilePath);
        return;
    }
    
    // Get file metadata to check size
    file:MetaData & readonly fileMetadata = check file:getMetaData(localFilePath);
    int currentFileSize = fileMetadata.size;
    
    // Check if file size has changed
    int previousFileSize = 0;
    string fileName = check file:basename(localFilePath);
    if lastFileSize.hasKey(fileName) {
        int? storedSize = lastFileSize[fileName];
        if storedSize is int {
            previousFileSize = storedSize;
        }
    }
    
    if currentFileSize != previousFileSize {
        log:printInfo("Local file modification detected", 
            fileName = fileName,
            previousSize = previousFileSize,
            currentSize = currentFileSize
        );
        
        // Update file size tracking
        lastFileSize[fileName] = currentFileSize;
        
        // Read and process the file
        error? processResult = processLocalCSVFile(fileName);
        if processResult is error {
            log:printError("Failed to process modified local CSV file", 'error = processResult);
        }
    } else {
        log:printInfo("No changes detected in local file", fileName = fileName, fileSize = currentFileSize);
    }
}

// Function to process the local CSV file
function processLocalCSVFile(string fileName) returns error? {
    
    log:printInfo("Starting to process local CSV file", fileName = fileName, filePath = localFilePath);
    
    // Read file content using io:fileReadString
    string csvContent = check io:fileReadString(localFilePath);
    log:printInfo("Local CSV file content retrieved", contentLength = csvContent.length());
    
    return processNewCSVLinesFromContent(fileName, csvContent);
}

// Function to process new lines from CSV content
function processNewCSVLinesFromContent(string fileName, string csvContent) returns error? {
    
    // Create regex pattern for line breaks and split content into lines
    string:RegExp lineBreakPattern = re `\r?\n`;
    string[] lines = lineBreakPattern.split(csvContent);
    
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
    
    log:printInfo("Local file parsing details", 
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
    
    log:printInfo("Local processing state", 
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
            log:printError("Failed to process CSV line from local file", lineNumber = i, 'error = lineResult);
        } else {
            newLinesProcessed += 1;
            log:printInfo("Processed local file line", lineNumber = i, content = line);
        }
    }
    
    // Update last processed data line count (excluding header)
    int currentDataLines = nonEmptyLines.length() - 1;
    lastProcessedLineCount[fileName] = currentDataLines;
    
    log:printInfo("Completed processing new CSV lines from local file", 
        fileName = fileName,
        newLinesProcessed = newLinesProcessed,
        totalDataLinesInFile = currentDataLines,
        lastProcessedDataLines = currentDataLines
    );
}

// Function to process individual CSV line
function processCSVLine(string csvLine, int lineNumber) returns error? {
    
    // Create regex pattern for comma separation and split CSV line
    string:RegExp commaPattern = re `,`;
    string[] fields = commaPattern.split(csvLine);
    
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
        sourceFile: localFilePath
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
    
    log:printInfo("New plan data published successfully from local file", 
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
        log:printWarn("No local polling task to stop - task may not be running");
    }
}

// Function to restart polling (useful for maintenance)
public function restartFilePolling() {
    // Stop existing polling if running
    stopFilePolling();
    
    // Start new polling task
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new LocalFilePollingJob(), 30);
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
    boolean fileExists = check file:test(localFilePath, file:EXISTS);
    if !fileExists {
        error fileNotFoundError = error("Local CSV file does not exist at: " + localFilePath);
        log:printError("Local file access test failed", 'error = fileNotFoundError);
        return fileNotFoundError;
    }
    
    // Check if file is readable
    boolean isReadable = check file:test(localFilePath, file:READABLE);
    if !isReadable {
        error notReadableError = error("Local CSV file is not readable: " + localFilePath);
        log:printError("Local file access test failed", 'error = notReadableError);
        return notReadableError;
    }
    
    // Try to read the file
    string|io:Error fileContent = io:fileReadString(localFilePath);
    if fileContent is io:Error {
        log:printError("Local file access test failed - cannot read file", 'error = fileContent);
        return fileContent;
    }
    
    log:printInfo("Local file access test successful", 
        filePath = localFilePath,
        contentLength = fileContent.length()
    );
}