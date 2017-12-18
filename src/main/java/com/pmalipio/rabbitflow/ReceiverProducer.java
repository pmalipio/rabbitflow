package com.pmalipio.rabbitflow;

import com.rabbitmq.client.*;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReceiverProducer<T> implements Flow.Publisher<T> {

    private static Logger log = LoggerFactory.getLogger(ReceiverProducer.class);
    private final ExecutorService executor;
    private final String host;
    private final String exchange;
    private final String routingKey;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private final Set<ReceiverSubscription> subscriptions = Collections.synchronizedSet(new HashSet<>());

    public ReceiverProducer(ExecutorService executor, String host, String exchange, String routingKey) {
        this.executor = executor;
        this.host = host;
        this.exchange = exchange;
        this.routingKey = routingKey;
        log.info("Rabbitmq receiver stream producer started!");
    }

    public ReceiverProducer(String host, String exchange, String routingKey) {
        this.executor = Executors.newFixedThreadPool(4);
        this.host = host;
        this.exchange = exchange;
        this.routingKey = routingKey;
        log.info("Rabbitmq receiver stream producer started!");
    }

    public void waitUntilTerminated() throws InterruptedException {
        try {
            terminated.get();
        } catch (ExecutionException ex) {
            log.error("Failed to wait until terminated: {}", ex);
        }
    }

    public ReceiverProducer subscribe(java.util.function.Consumer<T> onNext) {
        final ReceiverSubscriber<T> receiverSubscriber = new ReceiverSubscriber<>() {
            @Override
            public void onNext(T item) {
                super.onNext(item);
                onNext.accept((T) item);
            }
        };
        subscribe(receiverSubscriber);
        return this;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> subscriber) {
        try {
            ReceiverSubscription<T> receiverSubscription = new ReceiverSubscription<>(executor, subscriber, host, exchange, routingKey);
            subscriptions.add(receiverSubscription);
            subscriber.onSubscribe(receiverSubscription);
            log.info("Added a new subscriber!");
        } catch (Exception ex) {
            log.error("Could not start subscriber: {} ", ex);
        }
    }

    private class ReceiverSubscription<T> implements Flow.Subscription {
        private static final int MESSAGE_BUFFER_SIZE = 100;
        private static final long MESSAGE_BUFFER_TIMEOUT = 1000;

        private final Connection connection;
        private final Channel channel;
        private final BlockingQueue<T> messageBuffer;
        private final String queueName;
        private final Consumer consumer;
        private final ExecutorService executor;
        private Flow.Subscriber<? super T> subscriber;
        private AtomicBoolean isCanceled;

        public ReceiverSubscription(ExecutorService executor, Flow.Subscriber<? super T> subscriber,
                                    String host, String exchange, String routingKey) throws Exception {
            this.executor = executor;
            this.subscriber = subscriber;
            this.messageBuffer = new LinkedBlockingQueue<>(MESSAGE_BUFFER_SIZE);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(host);
            this.connection = factory.newConnection();
            this.channel = connection.createChannel();

            this.channel.exchangeDeclare(exchange, "fanout");
            this.queueName = channel.queueDeclare().getQueue();
            this.channel.queueBind(queueName, exchange, routingKey);
            this.isCanceled = new AtomicBoolean(false);

            this.consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    try {
                        T message = (T) SerializationUtils.deserialize(body);
                        try {
                            log.trace("Received message: " + message);
                            messageBuffer.offer(message, MESSAGE_BUFFER_TIMEOUT, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException ex) {
                            log.error("Message buffer timeout. Message {} is dropped.", message);
                        }
                    } catch (ClassCastException ex) {
                        log.error("Message failed to deserialize.");
                    }
                }
            };
            channel.basicConsume(queueName, true, consumer);
        }

        @Override
        public void request(long n) {
            if (isCanceled.get())
                return;
            if (n == Long.MAX_VALUE) {
              publishUnbound();
            } else if (n < 0)
                executor.execute(() -> subscriber.onError(new IllegalArgumentException()));
            else if (messageBuffer.size() > 0)
                publishItems(n);
            else {
                subscriber.onComplete();
            }
        }

        private void publishUnbound() {
            executor.submit(() -> {
                for (; ; )
                    subscriber.onNext(messageBuffer.take());
            });
        }

        private void publishItems(long n) {

            int remainItems = messageBuffer.size();

            if ((remainItems == n) || (remainItems > n)) {
                log.debug("Consuming " + n + " items to be published to Subscriber!");
                for (int i = 0; i < n; i++) {
                    executor.execute(() -> {
                        subscriber.onNext(messageBuffer.poll());
                    });
                }
                subscriber.onComplete();
                log.trace("Remaining " + (messageBuffer.size() - n) + " items to be published to Subscriber!");
            } else if ((remainItems > 0) && (remainItems < n)) {
                log.debug("Consuming " + n + " items to be published to Subscriber!");
                for (int i = 0; i < remainItems; i++) {
                    executor.execute(() -> {
                        subscriber.onNext(messageBuffer.poll());
                    });
                }
                subscriber.onComplete();
            } else {
                log.debug("Processor contains no item!");
            }
        }

        @Override
        public void cancel() {
            isCanceled.set(true);
            synchronized (subscriptions) {
                subscriptions.remove(this);
                if (subscriptions.size() == 0)
                    shutdown();
            }
        }

        private void shutdown() {
            executor.shutdown();
            try {
                channel.close();
                connection.close();
            } catch (Exception ex) {
                log.error("Error shutting down: {} ", ex);
            }
        }
    }
}
