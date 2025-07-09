import ballerina/log;

public function main() {
    log:printInfo("Telecom integration service started");
    log:printInfo("HTTP service available at: http://localhost:8080/telecom");
    log:printInfo("Kafka consumer listening on topic: " + topicName);
    log:printInfo("FTP file listener monitoring: " + ftpHost + ":" + ftpPort.toString() + ftpFilePath);
    log:printInfo("Plan data will be published to topic: " + planTopicName);
}