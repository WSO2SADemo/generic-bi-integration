import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;

// HTTP service for telecom operations
service /telecom on new http:Listener(8080) {
    
    // Endpoint to register new customer
    function init() returns error? {
        log:printInfo("telecom Service initialised !");
    }  
    resource function post customers(CustomerData customerData) returns http:Response|error {
        
        // Create telecom message
        TelecomMessage telecomMessage = {
            messageId: uuid:createType1AsString(),
            timestamp: time:utcToString(time:utcNow()),
            customerData: customerData,
            eventType: "CUSTOMER_REGISTRATION"
        };
        
        // Publish to Kafka using phoneNumber as key since customerId is auto-generated
        kafka:AnydataProducerRecord producerRecord = {
            topic: topicName,
            value: telecomMessage,
            key: customerData.phoneNumber
        };
        
        kafka:Error? result = kafkaProducer->send(producerRecord);
        
        http:Response response = new;
        if result is kafka:Error {
            log:printError("Failed to publish customer data to Kafka", result);
            response.statusCode = 500;
            response.setTextPayload("Failed to process customer registration");
        } else {
            log:printInfo("Customer data published successfully", phoneNumber = customerData.phoneNumber);
            response.statusCode = 201;
            response.setJsonPayload({
                message: "Customer registered successfully",
                phoneNumber: customerData.phoneNumber,
                messageId: telecomMessage.messageId
            });
        }
        
        return response;
    }
    
    // Endpoint to update customer plan using phoneNumber as identifier
    resource function put customers/[string phoneNumber]/plan(record {string planType; decimal monthlyCharge;} planUpdate) returns http:Response|error {
        
        // Create plan update message
        CustomerData updatedCustomer = {
            phoneNumber: phoneNumber,
            planType: planUpdate.planType,
            monthlyCharge: planUpdate.monthlyCharge,
            status: "ACTIVE",
            registrationDate: ""
        };
        
        TelecomMessage telecomMessage = {
            messageId: uuid:createType1AsString(),
            timestamp: time:utcToString(time:utcNow()),
            customerData: updatedCustomer,
            eventType: "PLAN_UPDATE"
        };
        
        // Publish to Kafka using phoneNumber as key
        kafka:AnydataProducerRecord producerRecord = {
            topic: topicName,
            value: telecomMessage,
            key: phoneNumber
        };
        
        kafka:Error? result = kafkaProducer->send(producerRecord);
        
        http:Response response = new;
        if result is kafka:Error {
            log:printError("Failed to publish plan update to Kafka", result);
            response.statusCode = 500;
            response.setTextPayload("Failed to process plan update");
        } else {
            log:printInfo("Plan update published successfully", phoneNumber = phoneNumber);
            response.statusCode = 200;
            response.setJsonPayload({
                message: "Plan updated successfully",
                phoneNumber: phoneNumber,
                messageId: telecomMessage.messageId
            });
        }
        
        return response;
    }
}