package org.acme;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class MessageCollector {

    private final BlockingQueue<String> messages = new LinkedBlockingQueue<>();

    @Incoming("in")
    void consume(String message) {
        messages.add(message);
    }

    public String poll(long timeout, TimeUnit unit) throws InterruptedException {
        return messages.poll(timeout, unit);
    }
}
