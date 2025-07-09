import ballerina/log;
import ballerina/sql;
import ballerinax/kafka;

// Kafka consumer service with proper SSL configuration
service on new kafka:Listener(
    bootstrapServers = kafkaBootstrapServers,
    groupId = "telecom-consumer-group",
    topics = [topicName],
    secureSocket = secureSocketConfig,
    securityProtocol = kafka:PROTOCOL_SSL,
    autoCommit = true,
    offsetReset = kafka:OFFSET_RESET_EARLIEST
) {
    
    remote function onConsumerRecord(kafka:AnydataConsumerRecord[] records) returns error? {
        
        foreach kafka:AnydataConsumerRecord kafkaRecord in records {
            log:printInfo("Received message from Kafka", 
                topic = kafkaRecord.offset.partition.topic,
                partition = kafkaRecord.offset.partition.partition,
                offset = kafkaRecord.offset.offset
            );
            
            // Log the raw payload for debugging
            anydata messageValue = kafkaRecord.value;
            string payloadTypeString = (typeof messageValue).toString();
            log:printInfo("Raw message payload type", payloadType = payloadTypeString);
            
            // Handle the message parsing with proper error handling
            TelecomMessage|error telecomMessage = parseTelecomMessage(messageValue);
            
            if telecomMessage is TelecomMessage {
                log:printInfo("Successfully processed telecom message", 
                    messageId = telecomMessage.messageId,
                    eventType = telecomMessage.eventType,
                    phoneNumber = telecomMessage.customerData.phoneNumber
                );
                
                // Insert customer data into MySQL database
                error? insertResult = insertCustomerData(telecomMessage.customerData, telecomMessage.eventType);
                if insertResult is error {
                    log:printError("Failed to insert customer data into database", 'error = insertResult);
                } else {
                    log:printInfo("Customer data successfully processed in database", 
                        phoneNumber = telecomMessage.customerData.phoneNumber,
                        eventType = telecomMessage.eventType
                    );
                }
            } else {
                log:printError("Failed to parse telecom message", 'error = telecomMessage);
            }
        }
    }
    
    remote function onError(kafka:Error kafkaError) returns error? {
        log:printError("Kafka consumer error occurred", 'error = kafkaError);
    }
}

// Function to insert customer data into MySQL database
function insertCustomerData(CustomerData customerData, string eventType) returns error? {
    
    if eventType == "CUSTOMER_REGISTRATION" {
        // Insert new customer record (customerId will be auto-generated)
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO CustomerData (phoneNumber, planType, monthlyCharge, status, registrationDate)
            VALUES (${customerData.phoneNumber}, ${customerData.planType}, 
                    ${customerData.monthlyCharge}, ${customerData.status}, ${customerData.registrationDate})
            ON DUPLICATE KEY UPDATE
                planType = VALUES(planType),
                monthlyCharge = VALUES(monthlyCharge),
                status = VALUES(status),
                registrationDate = VALUES(registrationDate)
        `;
        
        sql:ExecutionResult|sql:Error result = mysqlClient->execute(insertQuery);
        if result is sql:Error {
            return error("Failed to insert customer registration data: " + result.message());
        }
        
        int? affectedRowCount = result.affectedRowCount;
        string|int? lastInsertId = result.lastInsertId;
        log:printInfo("Customer registration data inserted successfully", 
            phoneNumber = customerData.phoneNumber,
            affectedRows = affectedRowCount,
            generatedCustomerId = lastInsertId
        );
        
    } else if eventType == "PLAN_UPDATE" {
        // Update existing customer's plan using phoneNumber as identifier
        sql:ParameterizedQuery updateQuery = `
            UPDATE CustomerData 
            SET planType = ${customerData.planType}, 
                monthlyCharge = ${customerData.monthlyCharge},
                status = ${customerData.status}
            WHERE phoneNumber = ${customerData.phoneNumber}
        `;
        
        sql:ExecutionResult|sql:Error result = mysqlClient->execute(updateQuery);
        if result is sql:Error {
            return error("Failed to update customer plan data: " + result.message());
        }
        
        int? affectedRowCount = result.affectedRowCount;
        if affectedRowCount == 0 {
            log:printWarn("No customer found to update", phoneNumber = customerData.phoneNumber);
        } else {
            log:printInfo("Customer plan updated successfully", 
                phoneNumber = customerData.phoneNumber,
                affectedRows = affectedRowCount
            );
        }
    }
}