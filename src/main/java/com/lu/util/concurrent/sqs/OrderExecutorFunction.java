package com.lu.util.concurrent.sqs;

import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.sqs.AmazonSQSResponder;
import com.amazonaws.services.sqs.AmazonSQSResponderClientBuilder;
import com.amazonaws.services.sqs.MessageContent;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.lu.order.Order;
import com.lu.util.concurrent.CannotExecuteOrderException;
import com.lu.util.concurrent.OrderExecuter;

public class OrderExecutorFunction implements RequestHandler<SQSEvent, Void> {

    private static final AmazonSQSResponder sqs = 
            AmazonSQSResponderClientBuilder.defaultClient();
    
    private static final OrderExecuter EXECUTOR = new OrderExecuter();
    
    @Override
    public Void handleRequest(SQSEvent input, Context context) {
        SQSMessage message = input.getRecords().get(0);
        
        Order order = OrderSerializer.deserializeOrder(message.getBody());
        try {
            UUID uuid = EXECUTOR.submitOrder(order);

            MessageContent requestMessage = new MessageContent(message.getBody(), 
                    message.getMessageAttributes().entrySet().stream().collect(
                            Collectors.toMap(Entry::getKey, e -> toMessageAttributeValue(e.getValue()))));
            MessageContent response = new MessageContent(uuid.toString());
            sqs.sendResponseMessage(requestMessage, response);
        } catch (CannotExecuteOrderException|TimeoutException e) {
            // TODO-RS: serialize exception
            e.printStackTrace();
        }
        return null;
    }

    private MessageAttributeValue toMessageAttributeValue(MessageAttribute attribute) {
        return new MessageAttributeValue().withDataType(attribute.getDataType())
                                          .withStringValue(attribute.getStringValue())
                                          .withStringListValues(attribute.getStringListValues())
                                          .withBinaryValue(attribute.getBinaryValue())
                                          .withBinaryListValues(attribute.getBinaryListValues());
    }
    
}
