// Kafka broker configuration
configurable string kafkaBootstrapServers = ?;
configurable string accessCertPath = ?;
configurable string accessKeyPath = ?;
configurable string caCertPath = ?;
configurable string topicName = ?;
configurable string planTopicName = ?;

// Verification and SMS service configuration
configurable string verificationServiceUrl = ?;
configurable string smsServiceUrl = ?;

// OAuth2 configuration for verification service
configurable string verificationOAuth2TokenUrl = ?;
configurable string verificationOAuth2ClientId = ?;
configurable string verificationOAuth2ClientSecret = ?;
configurable string verificationOAuth2Scopes = ?;

// OAuth2 configuration for SMS service
configurable string smsOAuth2TokenUrl = ?;
configurable string smsOAuth2ClientId = ?;
configurable string smsOAuth2ClientSecret = ?;
configurable string smsOAuth2Scopes = ?;