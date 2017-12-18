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
package com.pmalipio.rabbitflow.examples;

import com.pmalipio.rabbitflow.ReceiverProducer;

class FlowReceiver {

    public static void main(final String[] argv) {
        ReceiverProducer receiverProducer = new ReceiverProducer<>("172.17.0.2", "ex", "")
                .subscribe(m -> System.out.println("S1 got message: " + m));
    }
}
