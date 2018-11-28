package com.lu.util.concurrent.sqs;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSRequester;
import com.amazonaws.services.sqs.AmazonSQSRequesterClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.lu.order.Order;
import com.lu.util.concurrent.CannotExecuteOrderException;
import com.lu.util.concurrent.IOrderExecuter;

public class SQSOrderExecutorClient implements IOrderExecuter {

	public static final int MAX_TIMEOUT = 10;

	private final AmazonSQSRequester sqs;
	private final String requestQueueUrl;
	
	public SQSOrderExecutorClient(AmazonSQSRequester sqs, String requestQueueUrl) {
		this.sqs = sqs;
		this.requestQueueUrl = requestQueueUrl;
	}
	
	@Override
	public CompletableFuture<UUID> submitOrderAsync(Order order) {
		SendMessageRequest sendRequest = new SendMessageRequest()
				.withQueueUrl(requestQueueUrl)
				.withMessageBody(OrderSerializer.serializeOrder(order));
		
		CompletableFuture<Message> future = sqs.sendMessageAndGetResponseAsync(sendRequest, MAX_TIMEOUT, TimeUnit.SECONDS);
		
		return future.thenApply(OrderSerializer::deserializeResponse);
	}

	@Override
	public UUID submitOrder(Order order) throws TimeoutException, CannotExecuteOrderException {
		SendMessageRequest request = new SendMessageRequest()
				.withQueueUrl(requestQueueUrl)
				.withMessageBody(OrderSerializer.serializeOrder(order));
		
		Message reply = sqs.sendMessageAndGetResponse(request, MAX_TIMEOUT, TimeUnit.SECONDS);
		
		return OrderSerializer.deserializeResponse(reply);
	}
	
	@Override
	public void shutdown() {
		this.sqs.shutdown();
	}
	
	public static void main(String[] args) throws Exception {
		String requestQueueUrl = args[0];
		System.out.println("Starting up client using queue: " + requestQueueUrl);
		
		AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                                              .withRegion(Regions.US_EAST_1)
                                              .build();
		AmazonSQSRequester requester = AmazonSQSRequesterClientBuilder.standard()
		                                        .withAmazonSQS(sqs)
		                                        .build();

		IOrderExecuter executor = new SQSOrderExecutorClient(requester, requestQueueUrl);
		
		Random random = ThreadLocalRandom.current();
		
		ShutdownUtils.repeatUntilShutdown(() -> {
			Thread.sleep(random.nextInt(3000) + 3000L);

			try {
				submitOrder(executor, random);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return null;
		});
		
		executor.shutdown();
	}

	private static void submitOrder(IOrderExecuter executor, Random random) throws InterruptedException, ExecutionException {
		Order order = new Order(random.nextInt(10));
		int numEntries = random.nextInt(5) + 1; 
		IntStream.range(0, numEntries).forEach(i -> {
			order.AddEntry(random.nextInt(1000000));
		});
		
		System.out.println("Sending order: " + order);
		try {
			UUID uuid = executor.submitOrder(order);
			System.out.println("Received response: " + uuid);
		} catch (CannotExecuteOrderException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			System.out.println("Got tired of waiting for response :(");
		}
	}
}
