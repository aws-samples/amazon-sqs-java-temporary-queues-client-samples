package com.amazonaws.services.sqs;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * A Lambda that invokes the SQSIdentityFunction using the SQS request client.
 */
public class SQSIdentityFunctionRequester implements RequestHandler<String, String> {

    private static final AmazonSQSRequester requester = AmazonSQSRequesterClientBuilder.defaultClient();
    
    private static final String SQS_IDENTITY_FUNCTION_QUEUE_URL = System.getenv("SQS_IDENTITY_FUNCTION_QUEUE_URL");
    
    @Override
    public String handleRequest(String input, Context context) {
        SendMessageRequest sendRequest = new SendMessageRequest()
            .withQueueUrl(SQS_IDENTITY_FUNCTION_QUEUE_URL)
            .withMessageBody(input);
        try {
            Message response = requester.sendMessageAndGetResponse(sendRequest, 30, TimeUnit.SECONDS);
            String result = response.getBody();
            if (!input.equals(result)) {
                throw new RuntimeException("Incorrect result: " + result);
            }
            return result;
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
