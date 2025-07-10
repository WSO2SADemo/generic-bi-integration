import ballerina/ftp;
import ballerina/http;
import ballerinax/kafka;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

// Kafka producer client
public final kafka:Producer kafkaProducer = check new (
    bootstrapServers = kafkaBootstrapServers,
    securityProtocol = kafka:PROTOCOL_SSL,
    secureSocket = secureSocketConfig
);

// MySQL database client with SSL configuration for cloud databases
public final mysql:Client mysqlClient = check new (
    host = mysqlHost,
    user = mysqlUsername,
    password = mysqlPassword,
    database = mysqlDatabase,
    port = mysqlPort,
    options = {
        ssl: {
            mode: mysql:SSL_REQUIRED
        },
        connectTimeout: 30,
        socketTimeout: 0
    }
);

// FTP client for file operations
public final ftp:Client ftpClient = check new ({
    protocol: ftp:FTP,
    host: ftpHost,
    port: ftpPort,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    }
});

// HTTP client for verification service
public final http:Client verificationClient = check new (verificationServiceUrl, {
    timeout: 30
});

// HTTP client for SMS service
public final http:Client smsClient = check new (smsServiceUrl, {
    timeout: 30
});