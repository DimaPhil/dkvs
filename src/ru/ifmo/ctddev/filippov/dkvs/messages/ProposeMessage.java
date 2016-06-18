package ru.ifmo.ctddev.filippov.dkvs.messages;

import ru.ifmo.ctddev.filippov.dkvs.OperationDescriptor;

/**
 * Created by dmitry on 03.06.16.
 */
public class ProposeMessage extends Message.LeaderMessage {
    public int slot;
    public OperationDescriptor request;

    public ProposeMessage(int fromId, int slot, OperationDescriptor descriptor) {
        super(fromId);
        this.slot = slot;
        this.request = descriptor;
    }

    @Override
    public String toString() {
        return String.format("propose %d %d %s", fromId, slot, request);
    }
}