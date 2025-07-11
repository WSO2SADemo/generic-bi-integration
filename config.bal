// Kafka broker configuration
configurable string kafkaBootstrapServers = ?;
configurable string accessCertPath = ?;
configurable string accessKeyPath = ?;
configurable string caCertPath = ?;
configurable string topicName = ?;
configurable string planTopicName = ?;
configurable string planTopicName1 = ?;

// MySQL database configuration
configurable string mysqlHost = ?;
configurable string mysqlUsername = ?;
configurable string mysqlPassword = ?;
configurable string mysqlDatabase = ?;
configurable int mysqlPort = ?;

// FTP configuration
configurable string ftpHost = ?;
configurable int ftpPort = ?;
configurable string ftpUsername = ?;
configurable string ftpPassword = ?;
configurable string ftpFilePath = ?;

// Verification and SMS service configuration
configurable string verificationServiceUrl = ?;
configurable string smsServiceUrl = ?;

