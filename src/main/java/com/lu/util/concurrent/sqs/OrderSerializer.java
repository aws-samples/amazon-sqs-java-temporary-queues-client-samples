package com.lu.util.concurrent.sqs;

import java.io.IOException;
import java.util.UUID;

import com.amazonaws.services.sqs.executors.Base64Serializer;
import com.amazonaws.services.sqs.executors.DefaultSerializer;
import com.amazonaws.services.sqs.executors.InvertibleFunction;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lu.order.Order;
import com.lu.util.concurrent.CannotExecuteOrderException;

public class OrderSerializer {

	private static final String EXCEPTION_ATTRIBUTE_NAME = "Exception";

	private static final InvertibleFunction<Object, String> DEFAULT_SERIALIZER = 
	        DefaultSerializer.INSTANCE.andThen(Base64Serializer.INSTANCE);
	
	private static final ObjectMapper MAPPER = new ObjectMapper();
	
	public static String serializeOrder(Order order) throws CannotExecuteOrderException {
		try {
			return MAPPER.writeValueAsString(order);
		} catch (JsonProcessingException e) {
			throw new CannotExecuteOrderException("Could not serialize order", e);
		}
	}
	
	public static Order deserializeOrder(String serialized) {
		try {
			return MAPPER.readValue(serialized, Order.class);
		} catch (IOException e) {
			throw new CannotExecuteOrderException("Could not deserialize order", e);
		}
	}
	
	public static Message serializeOrderProcessingResult(Object result) {
		Message message = new Message();
		if (result instanceof UUID) {
			message.setBody(serializeUUID((UUID)result));
		} else if (result instanceof CannotExecuteOrderException) {
			message.setBody(serializeException((CannotExecuteOrderException)result));
			message.addMessageAttributesEntry(OrderSerializer.EXCEPTION_ATTRIBUTE_NAME,
    				new MessageAttributeValue().withDataType("String").withStringValue(Boolean.TRUE.toString()));
		} else {
			throw new IllegalArgumentException("Unrecognized result type: " + result);
		}
		return message;
	}
	
	public static UUID deserializeResponse(Message message) {
		if (message.getMessageAttributes().containsKey(EXCEPTION_ATTRIBUTE_NAME)) {
			throw deserializeException(message.getBody());
		} else {
			return deserializeUUID(message.getBody());
		}
	}
	
	public static String serializeUUID(UUID uuid) {
		return uuid.toString();
	}
	
	public static UUID deserializeUUID(String serialized) {
		return UUID.fromString(serialized);
	}
	
	public static String serializeException(CannotExecuteOrderException exception) {
		return DEFAULT_SERIALIZER.apply(exception);
	}
	
	public static CannotExecuteOrderException deserializeException(String serialized) {
		return (CannotExecuteOrderException)DEFAULT_SERIALIZER.unapply(serialized);
	}
	
}
