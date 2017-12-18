package com.pmalipio.rabbitflow.test;

import com.pmalipio.rabbitflow.ReceiverProducer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang.SerializationUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

class RabbitFlowTest {
    private static final String EXCHANGE_NAME = "ex";

    @Test
    void basicTest() {
        final String message = "Test message";

        final CountDownLatch latch = new CountDownLatch(1);

        final ReceiverProducer receiverProducer = new ReceiverProducer<>("172.17.0.2", EXCHANGE_NAME, "")
                .subscribe(m -> {
                    assertThat(m).isEqualTo(message);
                    latch.countDown();
                });

        sendMessage(message);

        try {
            if (!latch.await(100, TimeUnit.MILLISECONDS)) {
                fail("Timeout");
            }
        } catch (InterruptedException e) {
            fail("Failed to receive message in all subscribers");
        }
    }

    @Test
    void test2Subscribers() {

        final String message = "Test message";

        final CountDownLatch latch = new CountDownLatch(2);

        final ReceiverProducer receiverProducer = new ReceiverProducer<>("172.17.0.2", EXCHANGE_NAME, "")
                .subscribe(m -> {
                    assertThat(m).isEqualTo(message);
                    latch.countDown();
                })
                .subscribe(m -> {
                    assertThat(m).isEqualTo(message);
                    latch.countDown();
                });

        try {
            sendMessage(message);
        } catch (Exception ex) {
            fail("Could not send message.");
        }

        try {
            if (!latch.await(100, TimeUnit.MILLISECONDS)) {
                fail("Timeout");
            }
        } catch (InterruptedException e) {
            fail("Failed to receive message in all subscribers");
        }
    }

    @Test
    void testCongestion() {
        final String message = "Test message";

        final int numberOfMessages = 1000;

        final CountDownLatch latch = new CountDownLatch(numberOfMessages);

        final ExecutorService executor = Executors.newSingleThreadExecutor();

        final ReceiverProducer receiverProducer = new ReceiverProducer<>("172.17.0.2", EXCHANGE_NAME, "")
                .subscribe(m -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                });

        sendMessages(numberOfMessages, message);

        try {
            if (!latch.await(1, TimeUnit.MINUTES)) {
                fail("Timeout");
            }
        } catch (InterruptedException e) {
            fail("Failed to receive message in all subscribers");
        }
    }

    private void sendMessage(final String message) {
        sendMessages(1, message);
    }

    private void sendMessages(final int numberOfMessages, final String message) {
        try {
            final ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("172.17.0.2");
            final Connection connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            final byte[] serializedMsg = SerializationUtils.serialize(message);

            IntStream.range(0, numberOfMessages)
                    .forEach(i -> {
                        try {
                            channel.basicPublish(EXCHANGE_NAME, "", null, serializedMsg);
                        } catch (Exception ex) {
                            fail("failed to publish message " + message);
                        }
                    });

            channel.close();
            connection.close();
        } catch (Exception ex) {
            fail("Could not setup sender.");
        }
    }
}
