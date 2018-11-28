package com.lu.util.concurrent;

import com.lu.order.Order;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public interface IOrderExecuter {
    CompletableFuture<UUID> submitOrderAsync(Order order) throws CannotExecuteOrderException;

    UUID submitOrder(Order order) throws TimeoutException, CannotExecuteOrderException;

    void shutdown();
}
