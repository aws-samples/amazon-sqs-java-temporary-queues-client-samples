package com.lu.util.concurrent;


import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.lu.order.Order;


public class OrderExecuter implements IOrderExecuter {
    public static final int NUMBER_OF_THREADS = 10;
    public static final int MAX_TIMEOUT = 10;

    private final ExecutorService threadPool
            = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

    @Override
    public CompletableFuture<UUID> submitOrderAsync(Order order) // This will never actually throw CannotExecuteOrderException itself!
    {
        order.validate();
        
        System.out.println(String.format("Submitting an order for Customer %d with %d items", order.getCustomerId(), order.GetNumberOfEntries()));
        return CompletableFuture.supplyAsync(() -> {
            try {
                // simpulate long running call by introducing a delay that equals to NumberOfEntries seconds with a max of 10s.
                // The bigger the order the longer the delay up to 10s
                int timeout = Math.min(10, order.GetNumberOfEntries());
                TimeUnit.SECONDS.sleep(timeout);
            } catch (InterruptedException e) {
                throw new CannotExecuteOrderException("Order submission interrupted", e);
            }
            return UUID.randomUUID();
        }, threadPool);
    }


    @Override
    public UUID submitOrder(Order order) throws TimeoutException, CannotExecuteOrderException {
        CompletableFuture<UUID> futureOrder = submitOrderAsync(order);
        UUID orderId;
        try {
            // max timeout is 10 seconds
            orderId = futureOrder.get(MAX_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new CannotExecuteOrderException("Cannot submit order", e);
        }

        return orderId;
    }

    @Override
    public void shutdown()
    {
        threadPool.shutdown();
    }
}
