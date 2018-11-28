package com.lu.util.concurrent.sqs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSRequester;
import com.amazonaws.services.sqs.AmazonSQSRequesterClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSResponder;
import com.amazonaws.services.sqs.AmazonSQSResponderClientBuilder;
import com.lu.order.Order;
import com.lu.util.concurrent.CannotExecuteOrderException;
import com.lu.util.concurrent.IOrderExecuter;

public class SQSOrderExecutorTest {
    private static AmazonSQS sqs;
    private static AmazonSQSRequester requester;
    private static AmazonSQSResponder responder;
    private static String requestQueueUrl;
    private static SQSOrderExecutorServer server;
    
    @Before
    public void setup() {
        sqs = AmazonSQSClientBuilder.defaultClient();
        requestQueueUrl = sqs.createQueue("SQSOrderExecutorTest-RequestQueue-" + UUID.randomUUID().toString()).getQueueUrl();
        requester = AmazonSQSRequesterClientBuilder.standard().withAmazonSQS(sqs).build();
        responder = AmazonSQSResponderClientBuilder.standard().withAmazonSQS(sqs).build();
        server = new SQSOrderExecutorServer(responder, requestQueueUrl);
        server.start();
    }
    
    @After
    public void teardown() throws InterruptedException {
        if (server != null) {
            server.shutdown();
        }
        if (requestQueueUrl != null) {
            sqs.deleteQueue(requestQueueUrl);
        }
        if (requester != null) {
            requester.shutdown();
        }
        if (responder != null) {
            responder.shutdown();
        }
    }
    
    @Test
    public void roundTripHappyPathSynchronous() throws Exception {
        Order order = new Order(12345);
        order.AddEntry(42);
        order.AddEntry(12);
        order.AddEntry(1000000);
        
        IOrderExecuter client = new SQSOrderExecutorClient(requester, requestQueueUrl);
        UUID uuid = client.submitOrder(order);
        
        assertNotNull(uuid);
    }
    
    @Test
    public void roundTripInvalidOrderSynchronous() throws Exception {
        Order order = new Order(-5);
        order.AddEntry(42);
        order.AddEntry(12);
        order.AddEntry(1000000);
        
        IOrderExecuter client = new SQSOrderExecutorClient(requester, requestQueueUrl);
        try {
            client.submitOrder(order);
            fail("Exception expected");
        } catch (CannotExecuteOrderException e) {
            assertEquals("customerId must be positive: -5", e.getMessage());
        }
    }
}
