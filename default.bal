import ballerinax/kafka;

// Kafka SSL configuration
public final kafka:SecureSocket secureSocketConfig = {
    cert: caCertPath,
    'key: {
        certFile: accessCertPath,
        keyFile: accessKeyPath
    },
    protocol: {
        name: "TLS"
    }
};