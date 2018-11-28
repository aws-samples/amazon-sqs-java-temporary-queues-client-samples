package com.lu.util.concurrent.sqs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class ShutdownUtils {

	public static void repeatUntilShutdown(Callable<?> callable) throws Exception {
		CountDownLatch shuttingDown = new CountDownLatch(1);
		
		Runtime.getRuntime().addShutdownHook(new Thread(shuttingDown::countDown));
		
		Thread shutterDowner = new Thread((Runnable)(() -> {
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			try {
				while (!"exit".equals(reader.readLine())) {}
			} catch (IOException e) {
				// Ignore
			}
			shuttingDown.countDown();
		}), "ShutterDowner");
		shutterDowner.start();
		
		while (shuttingDown.getCount() > 0) {
			callable.call();
		}
	}
}
