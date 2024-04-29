/*
 * Copyright Broker QE authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.brokerqe.claire.clients.bundled;

public enum ArtemisCommand {
    ADDRESS_SHOW("address show"),
    ADDRESS_CREATE("address create"),
    ADDRESS_DELETE("address delete"),
    QUEUE_CREATE("queue create"),
    QUEUE_DELETE("queue delete"),
    QUEUE_STAT("queue stat"),
    PERF_CLIENT("perf client"),
    PERF_PRODUCER("perf producer"),
    PERF_CONSUMER("perf consumer");
//    BROWSE,
//    DATA,
//    TRANSFER;


    private final String command;

    ArtemisCommand(String command) {
        this.command = command;
    }

    public String getCommand() {
        return command;
    }
}
