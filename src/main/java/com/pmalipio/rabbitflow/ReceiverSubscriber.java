package com.pmalipio.rabbitflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;

public class ReceiverSubscriber<T> implements Flow.Subscriber<T>{

    private static Logger log = LoggerFactory.getLogger(ReceiverSubscriber.class);

    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        this.subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(T item) {
        log.debug("Consumed message: {}", item);
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Got error: {}", throwable);
    }

    @Override
    public void onComplete() {
        log.debug("Completed ");
    }

    public void cancel() {
        subscription.cancel();
    }
}
