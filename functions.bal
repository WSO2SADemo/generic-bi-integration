import ballerina/log;

// Helper function to parse telecom message from different formats
public function parseTelecomMessage(anydata messageValue) returns TelecomMessage|error {
    
    // Check if the message is a byte array (int[])
    if messageValue is int[] {
        log:printInfo("Message received as byte array, converting to string");
        
        // Convert int array to byte array
        byte[] byteArray = [];
        foreach int byteValue in messageValue {
            byteArray.push(<byte>byteValue);
        }
        
        // Convert bytes to string using string:fromBytes
        string|error jsonString = string:fromBytes(byteArray);
        if jsonString is error {
            return error("Failed to convert bytes to string: " + jsonString.message());
        }
        
        log:printInfo("Converted JSON string", jsonString = jsonString);
        
        // Parse JSON string
        json|error messageJson = jsonString.fromJsonString();
        if messageJson is error {
            return error("Failed to parse JSON string: " + messageJson.message());
        }
        
        // Convert to TelecomMessage
        return messageJson.cloneWithType(TelecomMessage);
    }
    
    // Handle JSON format
    json messageJson = messageValue.toJson();
    
    // Check if the message is a JSON array
    if messageJson is json[] {
        log:printInfo("Message is JSON array, extracting first element");
        if messageJson.length() > 0 {
            json firstElement = messageJson[0];
            return firstElement.cloneWithType(TelecomMessage);
        } else {
            return error("Empty JSON array received");
        }
    }
    
    // If it's not an array, treat it as a direct JSON object
    return messageJson.cloneWithType(TelecomMessage);
}

// Helper function to parse plan message from different formats
public function parsePlanMessage(anydata messageValue) returns PlanMessage|error {
    
    // Check if the message is a byte array (int[])
    if messageValue is int[] {
        log:printInfo("Plan message received as byte array, converting to string");
        
        // Convert int array to byte array
        byte[] byteArray = [];
        foreach int byteValue in messageValue {
            byteArray.push(<byte>byteValue);
        }
        
        // Convert bytes to string using string:fromBytes
        string|error jsonString = string:fromBytes(byteArray);
        if jsonString is error {
            return error("Failed to convert bytes to string: " + jsonString.message());
        }
        
        log:printInfo("Converted plan JSON string", jsonString = jsonString);
        
        // Parse JSON string
        json|error messageJson = jsonString.fromJsonString();
        if messageJson is error {
            return error("Failed to parse JSON string: " + messageJson.message());
        }
        
        // Convert to PlanMessage
        return messageJson.cloneWithType(PlanMessage);
    }
    
    // Handle JSON format
    json messageJson = messageValue.toJson();
    
    // Check if the message is a JSON array
    if messageJson is json[] {
        log:printInfo("Plan message is JSON array, extracting first element");
        if messageJson.length() > 0 {
            json firstElement = messageJson[0];
            return firstElement.cloneWithType(PlanMessage);
        } else {
            return error("Empty JSON array received");
        }
    }
    
    // If it's not an array, treat it as a direct JSON object
    return messageJson.cloneWithType(PlanMessage);
}