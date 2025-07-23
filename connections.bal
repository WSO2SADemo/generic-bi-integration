import ballerinax/kafka;
import ballerinax/mysql.driver as _;

// Kafka SSL configuration (moved here to avoid circular dependency)
final kafka:SecureSocket secureSocketConfig = {
    cert: caCertPath,
    'key: {
        certFile: accessCertPath,
        keyFile: accessKeyPath
    },
    protocol: {
        name: "TLS"
    }
};

// Kafka producer client
public final kafka:Producer kafkaProducer = check new (
    bootstrapServers = kafkaBootstrapServers,
    securityProtocol = kafka:PROTOCOL_SSL,
    secureSocket = secureSocketConfig
);