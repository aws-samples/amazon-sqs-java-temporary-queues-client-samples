package com.amazonaws.services.sqs;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

/**
 * A Lambda that just returns its input. Useful for benchmarking purposes.
 */
public class IdentityFunction implements RequestHandler<String, String> {

    @Override
    public String handleRequest(String input, Context context) {
        return input;
    }
}
