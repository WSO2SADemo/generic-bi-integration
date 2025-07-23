import ballerina/http;

// Kafka producer client

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
