import ballerina/lang.runtime;
import ballerina/log;

public function main() {
    log:printInfo("Telecom integration service started");

    // Test local file access
    error? testResult = testLocalFileAccess();
    if testResult is error {
        log:printError("Local file access test failed during startup", 'error = testResult);
    }

    // Test Kafka connectivity once (without processing data)
    error? kafkaTest = testKafkaConnection();
    if kafkaTest is error {
        log:printError("Kafka connectivity test failed during startup", 'error = kafkaTest);
    }

    // Process all rows once during startup (this will set the tracking correctly)
    error? processAllResult = processAllRows();
    if processAllResult is error {
        log:printError("Failed to process all rows during startup", 'error = processAllResult);
    }

    log:printInfo("Startup completed - polling will handle future file changes");

    // Keep the main function running to allow polling to continue
    log:printInfo("Service is now running. Polling every 30 seconds for file changes.");
    log:printInfo("To test new data processing, modify the CSV file or add new rows.");

    // Keep the program running
    while true {
        runtime:sleep(60); // Sleep for 60 seconds
        log:printInfo("Service is running - waiting for file changes...");
    }
}