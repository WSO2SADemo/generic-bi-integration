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

// HTTP client for verification service with OAuth2 client credentials
public final http:Client verificationClient = check new (verificationServiceUrl, {
    timeout: 30,
    auth: {
        clientId: verificationOAuth2ClientId,
        clientSecret: verificationOAuth2ClientSecret,
        tokenUrl: verificationOAuth2TokenUrl,
        scopes: re ` `.split(verificationOAuth2Scopes)
    }
});

// HTTP client for SMS service with OAuth2 client credentials
public final http:Client smsClient = check new (smsServiceUrl, {
    timeout: 30,
    auth: {
        clientId: smsOAuth2ClientId,
        clientSecret: smsOAuth2ClientSecret,
        tokenUrl: smsOAuth2TokenUrl,
        scopes: re ` `.split(smsOAuth2Scopes)
    }
});