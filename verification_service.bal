import ballerina/http;
import ballerina/lang.runtime;
import ballerina/log;
import ballerinax/wso2.apim.catalog as _;

// HTTP service for customer verification and SMS alert integration
service /integration on new http:Listener(8082) {
    function init() returns error? {
        log:printInfo("integration Service initialised !");
    }

    // Endpoint to process customer verification and send SMS alert
    resource function post verify\-and\-alert(xml customerXml) returns http:Response|error {

        log:printInfo("Received XML payload for verification and SMS alert");

        // Convert XML to CustomerVerificationData
        CustomerVerificationData|error verificationData = convertXmlToVerificationData(customerXml);
        if verificationData is error {
            http:Response response = new;
            response.statusCode = 400;
            response.setJsonPayload({
                message: "Invalid XML format",
                'error: verificationData.message()
            });
            return response;
        }

        log:printInfo("Converted XML to verification data",
                customerId = verificationData.customerId,
                phoneNumber = verificationData.phoneNumber,
                accountType = verificationData.accountType
        );

        // Call verification service and log the result
        VerificationResponse|error verificationResult = callVerificationService(verificationData);

        // Add detailed logging for verification result with proper type checking
        if verificationResult is VerificationResponse {
            log:printInfo("Verification service returned SUCCESS",
                    customerId = verificationResult.customerId,
                    isVerified = verificationResult.isVerified,
                    verificationStatus = verificationResult.verificationStatus,
                    verificationToken = verificationResult.verificationToken,
                    accountType = verificationResult.accountType,
                    riskLevel = verificationResult.riskLevel,
                    expiresAt = verificationResult.expiresAt
            );
            log:printInfo("Full verification response", verificationResponse = verificationResult.toJson().toString());

            // Add delay to allow token synchronization between services
            log:printInfo("Waiting for token synchronization between services");
            runtime:sleep(2); // Wait 2 seconds for token to be available in SMS service

            // Continue with SMS service call
            log:printInfo("Customer verification successful",
                    customerId = verificationResult.customerId,
                    verificationToken = verificationResult.verificationToken,
                    verificationStatus = verificationResult.verificationStatus
            );

            // Call SMS alert service with retry mechanism
            SmsSuccessResponse|SmsErrorResponse|error smsResult = callSmsAlertServiceWithRetry(
                    verificationResult.verificationToken,
                    verificationResult.customerId,
                    verificationData.phoneNumber,
                    3 // Retry up to 3 times
            );

            http:Response response = new;

            if smsResult is SmsSuccessResponse {
                log:printInfo("SMS alert sent successfully",
                        messageId = smsResult.messageId,
                        recipient = smsResult.recipient,
                        status = smsResult.status
                );

                response.statusCode = 200;
                response.setJsonPayload({
                    message: "Customer verified and SMS alert sent successfully",
                    verification: verificationResult.toJson(),
                    smsAlert: smsResult.toJson()
                });

            } else if smsResult is SmsErrorResponse {
                log:printWarn("SMS alert failed after retries",
                        errorCode = smsResult.code,
                        errorMessage = smsResult.message
                );

                response.statusCode = 207; // Multi-status: verification succeeded, SMS failed
                response.setJsonPayload({
                    message: "Customer verified but SMS alert failed",
                    verification: verificationResult.toJson(),
                    smsError: smsResult.toJson()
                });

            } else if smsResult is error {
                // Explicitly check if smsResult is error type
                log:printError("Error calling SMS service", 'error = smsResult);

                response.statusCode = 500;
                response.setJsonPayload({
                    message: "Customer verified but SMS service error occurred",
                    verification: verificationResult.toJson(),
                    'error: smsResult.message()
                });
            }

            return response;

        } else {
            // verificationResult is error
            log:printError("Verification service returned ERROR", 'error = verificationResult);
            http:Response response = new;
            response.statusCode = 500;
            response.setJsonPayload({
                message: "Failed to verify customer",
                'error: verificationResult.message()
            });
            return response;
        }
    }
}

// Function to convert XML to CustomerVerificationData
function convertXmlToVerificationData(xml customerXml) returns CustomerVerificationData|error {

    // Extract customer data from XML using proper XML navigation
    xml customerIdElements = customerXml/<customerId>;
    xml phoneNumberElements = customerXml/<phoneNumber>;
    xml accountTypeElements = customerXml/<accountType>;

    string customerId = customerIdElements.data();
    string phoneNumber = phoneNumberElements.data();
    string accountType = accountTypeElements.data();

    if customerId.length() == 0 || phoneNumber.length() == 0 || accountType.length() == 0 {
        return error("Missing required fields in XML: customerId, phoneNumber, or accountType");
    }

    return {
        customerId: customerId,
        phoneNumber: phoneNumber,
        accountType: accountType
    };
}

// Function to call verification service
function callVerificationService(CustomerVerificationData verificationData) returns VerificationResponse|error {

    log:printInfo("Calling verification service",
            serviceUrl = verificationServiceUrl,
            endpoint = "/customer"
    );

    // Convert to JSON for verification service call
    json verificationPayload = {
        customerId: verificationData.customerId,
        phoneNumber: verificationData.phoneNumber,
        accountType: verificationData.accountType
    };

    log:printInfo("Verification service request payload", payload = verificationPayload.toString());

    // Call verification service
    http:Response|http:ClientError verificationResponse = verificationClient->post("/customer", verificationPayload);

    if verificationResponse is http:ClientError {
        log:printError("HTTP client error when calling verification service", 'error = verificationResponse);
        return error("Failed to call verification service: " + verificationResponse.message());
    }

    log:printInfo("Verification service HTTP response received",
            statusCode = verificationResponse.statusCode,
            reasonPhrase = verificationResponse.reasonPhrase
    );

    // Accept both 200 (OK) and 201 (Created) as successful responses
    if verificationResponse.statusCode != 200 && verificationResponse.statusCode != 201 {
        log:printError("Verification service returned error status",
                statusCode = verificationResponse.statusCode,
                reasonPhrase = verificationResponse.reasonPhrase
        );
        return error("Verification service returned error status: " + verificationResponse.statusCode.toString());
    }

    log:printInfo("Verification service returned success status",
            statusCode = verificationResponse.statusCode,
            reasonPhrase = verificationResponse.reasonPhrase
    );

    // Parse verification response
    json|http:ClientError responsePayload = verificationResponse.getJsonPayload();
    if responsePayload is http:ClientError {
        log:printError("Failed to extract JSON payload from verification response", 'error = responsePayload);
        return error("Failed to parse verification response: " + responsePayload.message());
    }

    log:printInfo("Raw verification service response payload", rawPayload = responsePayload.toString());

    // Convert to VerificationResponse record
    VerificationResponse|error verificationResult = responsePayload.cloneWithType(VerificationResponse);
    if verificationResult is error {
        log:printError("Failed to convert response to VerificationResponse record",
                'error = verificationResult,
                rawPayload = responsePayload.toString()
        );
        return error("Failed to convert verification response: " + verificationResult.message());
    }

    log:printInfo("Successfully converted to VerificationResponse record",
            customerId = verificationResult.customerId,
            isVerified = verificationResult.isVerified,
            verificationStatus = verificationResult.verificationStatus
    );

    return verificationResult;
}

// Function to call SMS alert service with retry mechanism
function callSmsAlertServiceWithRetry(string verificationToken, string customerId, string phoneNumber, int maxRetries) returns SmsSuccessResponse|SmsErrorResponse|error {

    int retryCount = 0;

    while retryCount < maxRetries {
        log:printInfo("Attempting SMS service call",
                attempt = retryCount + 1,
                maxAttempts = maxRetries,
                verificationToken = verificationToken
        );

        // Call SMS service and handle the result properly
        SmsSuccessResponse|SmsErrorResponse|error smsResult = callSmsAlertService(verificationToken, customerId, phoneNumber);

        // Handle success case
        if smsResult is SmsSuccessResponse {
            log:printInfo("SMS service call successful", attempt = retryCount + 1);
            return smsResult;
        }

        // Handle error response case
        if smsResult is SmsErrorResponse {
            // Check if it's a token-related error that might be resolved with retry
            if smsResult.code == "TOKEN_NOT_EXISTS" || smsResult.code == "TOKEN_NOT_FOUND" {
                retryCount += 1;
                if retryCount < maxRetries {
                    log:printWarn("Token not found, retrying SMS service call",
                            attempt = retryCount,
                            nextAttemptIn = "3 seconds"
                    );
                    runtime:sleep(3); // Wait 3 seconds before retry
                    continue;
                } else {
                    log:printError("SMS service failed after all retries",
                            finalAttempt = retryCount,
                            errorCode = smsResult.code
                    );
                    return smsResult;
                }
            } else {
                // Non-token related error, don't retry
                log:printError("SMS service returned non-retryable error", errorCode = smsResult.code);
                return smsResult;
            }
        }

        // Handle general error case
        if smsResult is error {
            retryCount += 1;
            if retryCount < maxRetries {
                log:printWarn("SMS service call failed, retrying",
                        attempt = retryCount,
                        'error = smsResult,
                        nextAttemptIn = "3 seconds"
                );
                runtime:sleep(3);
                continue;
            } else {
                log:printError("SMS service failed after all retries", finalAttempt = retryCount);
                return smsResult;
            }
        }
    }

    return error("SMS service failed after " + maxRetries.toString() + " attempts");
}

// Function to call SMS alert service
function callSmsAlertService(string verificationToken, string customerId, string phoneNumber) returns SmsSuccessResponse|SmsErrorResponse|error {

    log:printInfo("Calling SMS alert service",
            serviceUrl = smsServiceUrl,
            endpoint = "/alert"
    );

    // Create SMS alert request
    SmsAlertRequest smsRequest = {
        verificationToken: verificationToken,
        phoneNumber: phoneNumber,
        alertType: "VERIFICATION_COMPLETE",
        message: "Your account verification for customer ID " + customerId + " has been completed successfully."
    };

    log:printInfo("SMS service request payload", payload = smsRequest.toJson().toString());

    // Call SMS service
    http:Response|http:ClientError smsResponse = smsClient->post("/alert", smsRequest);

    if smsResponse is http:ClientError {
        log:printError("HTTP client error when calling SMS service", 'error = smsResponse);
        return error("Failed to call SMS service: " + smsResponse.message());
    }

    log:printInfo("SMS service HTTP response received",
            statusCode = smsResponse.statusCode,
            reasonPhrase = smsResponse.reasonPhrase
    );

    // Parse SMS response based on status code
    json|http:ClientError responsePayload = smsResponse.getJsonPayload();
    if responsePayload is http:ClientError {
        log:printError("Failed to extract JSON payload from SMS response", 'error = responsePayload);
        return error("Failed to parse SMS response: " + responsePayload.message());
    }

    log:printInfo("Raw SMS service response payload", rawPayload = responsePayload.toString());

    // Check response content structure instead of just HTTP status code
    // First try to convert to error response to detect error conditions
    SmsErrorResponse|error smsErrorResult = responsePayload.cloneWithType(SmsErrorResponse);
    if smsErrorResult is SmsErrorResponse {
        // Response has error structure, treat as error regardless of HTTP status
        log:printInfo("SMS service returned error response",
                errorCode = smsErrorResult.code,
                errorMessage = smsErrorResult.message
        );
        return smsErrorResult;
    }

    // If error conversion failed, try success response conversion
    SmsSuccessResponse|error smsSuccessResult = responsePayload.cloneWithType(SmsSuccessResponse);
    if smsSuccessResult is SmsSuccessResponse {
        // Response has success structure
        log:printInfo("SMS service returned success response",
                messageId = smsSuccessResult.messageId,
                status = smsSuccessResult.status
        );
        return smsSuccessResult;
    }

    // If both conversions failed, return error
    log:printError("Failed to convert SMS response to either success or error format",
            rawPayload = responsePayload.toString()
    );
    return error("SMS service returned unexpected response format");
}