import ballerina/ftp;
import ballerinax/kafka;
import ballerinax/mysql.driver as _;

// Kafka producer client
public final kafka:Producer kafkaProducer = check new (
    bootstrapServers = kafkaBootstrapServers,
    securityProtocol = kafka:PROTOCOL_SSL,
    secureSocket = secureSocketConfig
);

// SFTP client for file operations (changed from FTP to SFTP)
public final ftp:Client ftpClient = check new ({
    protocol: ftp:SFTP,
    host: ftpHost,
    port: ftpPort,
    auth: {
        credentials: {
            username: ftpUsername,
            password: ftpPassword
        }
    }
});