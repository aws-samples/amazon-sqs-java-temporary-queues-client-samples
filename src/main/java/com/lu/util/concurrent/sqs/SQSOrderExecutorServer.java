package com.lu.util.concurrent.sqs;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSRequester;
import com.amazonaws.services.sqs.AmazonSQSRequesterClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSResponder;
import com.amazonaws.services.sqs.AmazonSQSResponderClientBuilder;
import com.amazonaws.services.sqs.MessageContent;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.util.SQSMessageConsumer;
import com.amazonaws.services.sqs.util.SQSMessageConsumerBuilder;
import com.lu.order.Order;
import com.lu.util.concurrent.CannotExecuteOrderException;
import com.lu.util.concurrent.OrderExecuter;

public class SQSOrderExecutorServer implements Consumer<Message> {

	public static final int NUMBER_OF_THREADS = 10;
    public static final int MAX_TIMEOUT = 10;

    private final AmazonSQSResponder responseClient;
	private final SQSMessageConsumer consumer;
	private final OrderExecuter executer;
	
	
	public SQSOrderExecutorServer(AmazonSQSResponder responseClient, String requestQueueUrl) {
		this.responseClient = responseClient;
		this.consumer = SQSMessageConsumerBuilder.standard()
				.withAmazonSQS(responseClient.getAmazonSQS())
				.withQueueUrl(requestQueueUrl)
				.withConsumer(this)
				.build();
		this.executer = new OrderExecuter();
	}
	
	public void start() {
		consumer.start();
	}
	
	public void shutdown() {
		consumer.shutdown();
		executer.shutdown();
	}
	
    @Override
    public void accept(Message message) {
    	Message response;
    	try {
    		Order order = OrderSerializer.deserializeOrder(message.getBody());
    		System.out.println("Handling order: " + order);
    		UUID result = executer.submitOrder(order);
    		response = OrderSerializer.serializeOrderProcessingResult(result);
    	} catch (CannotExecuteOrderException|TimeoutException e) {
    		response = OrderSerializer.serializeOrderProcessingResult(e);
        }
    	
    	MessageContent reply = new MessageContent(response.getBody(), response.getMessageAttributes());
    	responseClient.sendResponseMessage(MessageContent.fromMessage(message), reply);
    }
    
    public static void main(String[] args) throws Exception {
		String queueUrl = args[0];
		System.out.println("Starting up server using queue: " + queueUrl);
		
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                                              .withRegion(Regions.US_EAST_1)
                                              .build();
		AmazonSQSResponder responder = AmazonSQSResponderClientBuilder.standard()
                                                                      .withAmazonSQS(sqs)
                                                                      .build();
		
		SQSOrderExecutorServer server = new SQSOrderExecutorServer(responder, queueUrl);
		server.start();
		
		ShutdownUtils.repeatUntilShutdown(() -> null);
		
		server.shutdown();
	}
}
