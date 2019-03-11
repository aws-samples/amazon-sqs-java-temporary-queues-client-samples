package com.amazonaws.services.sqs;

import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

/**
 * An SQS-triggered Lambda function that just returns the body of the
 * SQS message. If the message is a request sent by a Requester client,
 * it sends the response through SQS as well.
 */
public class SQSIdentityFunction implements RequestHandler<SQSEvent, String> {

    private static final AmazonSQSResponder sqs = 
            AmazonSQSResponderClientBuilder.defaultClient();
    
    @Override
    public String handleRequest(SQSEvent input, Context context) {
        SQSMessage message = input.getRecords().get(0);
        String messageBody = message.getBody();
        
        MessageContent requestMessage = new MessageContent(messageBody, 
                message.getMessageAttributes().entrySet().stream().collect(
                        Collectors.toMap(Entry::getKey, e -> toMessageAttributeValue(e.getValue()))));
        if (sqs.isResponseMessageRequested(requestMessage)) {
            MessageContent response = new MessageContent(messageBody);
            sqs.sendResponseMessage(requestMessage, response);
        }
        return messageBody;
    }

    private MessageAttributeValue toMessageAttributeValue(MessageAttribute attribute) {
        return new MessageAttributeValue().withDataType(attribute.getDataType())
                                          .withStringValue(attribute.getStringValue())
                                          .withStringListValues(attribute.getStringListValues())
                                          .withBinaryValue(attribute.getBinaryValue())
                                          .withBinaryListValues(attribute.getBinaryListValues());
    }
}
