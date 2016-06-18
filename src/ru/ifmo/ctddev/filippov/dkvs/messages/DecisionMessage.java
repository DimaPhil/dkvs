package ru.ifmo.ctddev.filippov.dkvs.messages;

import ru.ifmo.ctddev.filippov.dkvs.OperationDescriptor;

/**
 * Created by dmitry on 03.06.16.
 */
public class DecisionMessage extends ReplicaMessage {
    public int slot;
    public OperationDescriptor request;

    public DecisionMessage(int slot, OperationDescriptor descriptor) {
        super();
        this.slot = slot;
        this.request = descriptor;
    }

    @Override
    public String toString() {
        return String.format("decision %d %s", slot, request);
    }

}
