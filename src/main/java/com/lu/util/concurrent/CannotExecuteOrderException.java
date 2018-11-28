package com.lu.util.concurrent;

public class CannotExecuteOrderException extends RuntimeException  {
    public CannotExecuteOrderException(String errorMessage, Throwable err){
        super(errorMessage, err);
    }
}
