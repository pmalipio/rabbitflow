package com.pmalipio.rabbitflow.examples;

import com.rabbitmq.client.*;

import java.io.IOException;

class OldStyleReceiver {
    private static final String EXCHANGE_NAME = "ex";

    public static void main(final String[] argv) throws Exception {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.2");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        final String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(final String consumerTag, final Envelope envelope,
                                       final AMQP.BasicProperties properties, final byte[] body) throws IOException {
                final String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
