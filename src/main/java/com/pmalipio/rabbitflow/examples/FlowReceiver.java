package com.pmalipio.rabbitflow.examples;

import com.pmalipio.rabbitflow.ReceiverProducer;

class FlowReceiver {

    public static void main(final String[] argv) {
        ReceiverProducer receiverProducer = new ReceiverProducer<>("172.17.0.2", "ex", "")
                .subscribe(m -> System.out.println("S1 got message: " + m));
    }
}
