package com.pmalipio.rabbitflow.examples;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang.SerializationUtils;

public class Sender {
    private static final String EXCHANGE_NAME = "ex";

    public static void main(String[] argv)
            throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("172.17.0.2");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        String message = "Hello!";

        channel.basicPublish(EXCHANGE_NAME, "", null, SerializationUtils.serialize(message));
        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}
