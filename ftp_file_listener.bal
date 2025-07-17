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

// FTP listener service to monitor file changes (for new files only)
service on new ftp:Listener({
    protocol: ftp:FTP,
    host: ftpHost,
    port: ftpPort,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    },
    path: "/folder1",
    fileNamePattern: "data.csv"
}) {
    
    remote function onFileChange(ftp:WatchEvent & readonly watchEvent, ftp:Caller caller) returns error? {
        
        // Process only added files (FTP WatchEvent only supports addedFiles)
        foreach ftp:FileInfo addedFile in watchEvent.addedFiles {
            log:printInfo("New file detected", fileName = addedFile.name, filePath = addedFile.path);
            
            // Process the new file
            error? processResult = processNewCSVLines(addedFile.name);
            if processResult is error {
                log:printError("Failed to process new CSV lines", 'error = processResult);
            }
        }
    }
}

// Polling task to check for file modifications
task:JobId? pollingJobId = ();

// Initialize polling task when module loads
function init() {
    // Start polling task to check for file modifications every 10 seconds
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new FilePollingJob(), 10);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("File polling task started", interval = "10 seconds");
    } else {
        log:printError("Failed to start file polling task", 'error = result);
    }
}

// Job class for polling file changes
class FilePollingJob {
    
    public function execute() {
        // Check for modifications to data.csv
        error? checkResult = checkFileModifications("data.csv");
        if checkResult is error {
            log:printError("Error checking file modifications", 'error = checkResult);
        }
    }
}

// Function to check if file has been modified
function checkFileModifications(string fileName) returns error? {
    
    // Construct the relative path for the FTP client
    string relativePath = "folder1/" + fileName;
    log:printInfo("File " + relativePath + " polling started.");
    // Get file content from FTP server
    stream<byte[] & readonly, io:Error?> fileStream = check ftpClient->get(relativePath);
    log:printInfo("Polled!!");
    // Read file content
    byte[] fileContent = [];
    check fileStream.forEach(function(byte[] & readonly chunk) {
        foreach byte b in chunk {
            fileContent.push(b);
        }
    });
    
    // Convert bytes to string
    string csvContent = check string:fromBytes(fileContent);
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
        log:printInfo("File modification detected", 
            fileName = fileName,
            previousSize = previousFileSize,
            currentSize = currentFileSize
        );
        
        // Update file size tracking
        lastFileSize[fileName] = currentFileSize;
        
        // Process new lines
        error? processResult = processNewCSVLinesFromContent(fileName, csvContent);
        if processResult is error {
            log:printError("Failed to process modified CSV file", 'error = processResult);
        }
    }
}

// Function to process only new lines in CSV file
function processNewCSVLines(string fileName) returns error? {
    
    log:printInfo("Starting to process new lines in CSV file", fileName = fileName);
    
    // Construct the relative path for the FTP client
    string relativePath = "folder1/" + fileName;
    
    // Get file content from FTP server using relative path
    stream<byte[] & readonly, io:Error?> fileStream = check ftpClient->get(relativePath);
    
    // Read file content
    byte[] fileContent = [];
    check fileStream.forEach(function(byte[] & readonly chunk) {
        foreach byte b in chunk {
            fileContent.push(b);
        }
    });
    
    // Convert bytes to string
    string csvContent = check string:fromBytes(fileContent);
    log:printInfo("CSV file content retrieved", contentLength = csvContent.length());
    
    // Update file size tracking
    lastFileSize[fileName] = csvContent.length();
    
    return processNewCSVLinesFromContent(fileName, csvContent);
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
    
    log:printInfo("File parsing details", 
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
    
    log:printInfo("Processing state", 
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
            log:printError("Failed to process CSV line", lineNumber = i, 'error = lineResult);
        } else {
            newLinesProcessed += 1;
            log:printInfo("Processed line", lineNumber = i, content = line);
        }
    }
    
    // Update last processed data line count (excluding header)
    int currentDataLines = nonEmptyLines.length() - 1;
    lastProcessedLineCount[fileName] = currentDataLines;
    
    log:printInfo("Completed processing new CSV lines", 
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
    
    log:printInfo("New plan data published successfully", 
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
    log:printInfo("Reset file position tracking", fileName = fileName);
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
            log:printError("Failed to stop file polling task", 'error = result);
        } else {
            log:printInfo("File polling task stopped successfully");
            pollingJobId = (); // Reset to null after successful unscheduling
        }
    } else {
        log:printWarn("No polling task to stop - task may not be running");
    }
}

// Function to restart polling (useful for maintenance)
public function restartFilePolling() {
    // Stop existing polling if running
    stopFilePolling();
    
    // Start new polling task
    task:JobId|task:Error result = task:scheduleJobRecurByFrequency(new FilePollingJob(), 10);
    if result is task:JobId {
        pollingJobId = result;
        log:printInfo("File polling task restarted", interval = "10 seconds");
    } else {
        log:printError("Failed to restart file polling task", 'error = result);
    }
}