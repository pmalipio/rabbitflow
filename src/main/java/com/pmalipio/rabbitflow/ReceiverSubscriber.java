/*
 * Copyright (c) 2017.  Pedro Alípio, All Rights Reserved.
 *
 * This material is provided "as is", with absolutely no warranty expressed
 * or implied. Any use is at your own risk.
 *
 * Permission to use or copy this software for any purpose is hereby granted
 * without fee. Permission to modify the code and to distribute modified
 * code is also granted without any restrictions.
 */
package com.pmalipio.rabbitflow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Flow;

class ReceiverSubscriber<T> implements Flow.Subscriber<T>{

    private static final Logger log = LoggerFactory.getLogger(ReceiverSubscriber.class);

    private Flow.Subscription subscription;

    @Override
    public void onSubscribe(final Flow.Subscription subscription) {
        this.subscription = subscription;
        this.subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(final T item) {
        log.debug("Consumed message: {}", item);
    }

    @Override
    public void onError(final Throwable throwable) {
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
