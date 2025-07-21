import ballerina/log;

public function main() {
    log:printInfo("Telecom integration service started");
    
    // Test SFTP connection
    error? testResult = testSFTPConnection();
    if testResult is error {
        log:printError("SFTP connection test failed during startup", 'error = testResult);
    }
}