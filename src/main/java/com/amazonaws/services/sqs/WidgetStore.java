package com.amazonaws.services.sqs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class WidgetStore {

	private static boolean running = true;
	private static AmazonSQSRequester sqsRequester;
    private static String requestQueueUrl;
	
	public static void main(String[] args) throws Exception {
		requestQueueUrl = args[0];
		System.out.println("Starting up store using queue: " + requestQueueUrl);
		
		AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
		sqsRequester = AmazonSQSRequesterClientBuilder.standard()
                                                      .withAmazonSQS(sqs)
                                                      .build();
		
		shutdownHooks(sqs);
		
		while (running) {
			Thread.sleep(ThreadLocalRandom.current().nextInt(3000) + 3000);

			try {
				requestResponseLoop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private static void requestResponseLoop() throws InterruptedException, ExecutionException {
		String requestBody = "Dude, we need more widgets!";
		System.out.println("Sending request: " + requestBody);
		SendMessageRequest request = new SendMessageRequest()
			    .withQueueUrl(requestQueueUrl)
				.withMessageBody(requestBody);
		
		try {
			String responseBody = sqsRequester.sendMessageAndGetResponse(
					request, 5, TimeUnit.SECONDS).getBody();
			System.out.println("Received response: " + responseBody);
		} catch (TimeoutException e) {
			System.out.println("Got tired of waiting for response :(");
		}
	}
	
	private static void shutdownHooks(AmazonSQS sqs) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting down!");
			sqs.shutdown();
		}));
		
		Thread shutterDowner = new Thread((Runnable)(() -> {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			try {
				while (!reader.readLine().equals("exit"));
			} catch (Exception e) {
			}
			running = false;
			System.exit(0);
		}), "WidgetStoreShutterDowner");
		shutterDowner.start();
	}
}
