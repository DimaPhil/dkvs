package ru.ifmo.ctddev.filippov.dkvs;

import java.io.PrintStream;

/**
 * Created by dimaphil on 03.06.2016.
 */
class Logger {
    int id;
    private PrintStream log;

    Logger(int id) {
        this.id = id;
        this.log = System.err;
    }

    void logConnection(String where, String message) {
        log.println(String.format("Connection, node = %d: %s - %s", id, where, message));
    }

    void logMessageOut(String where, String message) {
        log.println(String.format("Message out, node = %d: %s - %s", id, where, message));
    }

    void logMessageIn(String where, String message) {
        log.println(String.format("Message in, node = %d: %s - %s", id, where, message));
    }

    void logPaxos(String where, String message) {
        log.println(String.format("Paxos, node = %d: %s - %s", id, where, message));
    }

    void logPaxos(String message) {
        log.println(String.format("Paxos, node = %d: %s", id, message));
    }

    void logError(String where, String message) {
        log.println(String.format("Error, node = %d: %s - %s", id, where, message));
    }
}