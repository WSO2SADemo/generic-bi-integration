import ballerina/lang.runtime;
import ballerina/log;

public function main() {
    log:printInfo("Telecom integration service started");

    // Test local file access
    error? testResult = testLocalFileAccess();
    if testResult is error {
        log:printError("Local file access test failed during startup", 'error = testResult);
    }

    log:printInfo("Startup completed - Local file polling will handle file changes");

    // Keep the main function running to allow polling to continue
    log:printInfo("Service is now running. Polling every 30 seconds for local file changes.");
    log:printInfo("To test new data processing, modify the CSV file at: " + localFilePath);

    // Keep the program running
    while true {
        runtime:sleep(60); // Sleep for 60 seconds
        log:printInfo("Service is running - waiting for local file changes...");
    }
}