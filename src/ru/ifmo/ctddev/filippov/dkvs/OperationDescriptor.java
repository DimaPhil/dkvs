package ru.ifmo.ctddev.filippov.dkvs;

import ru.ifmo.ctddev.filippov.dkvs.messages.ClientRequest;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by dimaphil on 04.06.2016.
 */
public class OperationDescriptor {
    private int operationId;
    private static volatile int nextId = 0;
    ClientRequest request;

    private int code(int curId, int nodeId) {
        return curId * Node.mainConfig.nodesCount() + nodeId;
    }

    private static synchronized int get() {
        return nextId++;
    }

    OperationDescriptor(int nodeId, ClientRequest request) {
        this.operationId = code(get(), nodeId);
        this.request = request;
    }

    private OperationDescriptor(ClientRequest request, int operationId) {
        this.operationId = operationId;
        this.request = request;
    }

    public static OperationDescriptor parse(String[] parts) {
        String[] tail = Arrays.copyOfRange(parts, 3, parts.length);
        String[] head = new String[1];
        head[0] = parts[1];
        String[] request = Stream.concat(Arrays.stream(head), Arrays.stream(tail)).toArray(String[]::new);

        return new OperationDescriptor(ClientRequest.parse(Integer.parseInt(parts[2]), request),
                Integer.parseInt(parts[0].substring(1, parts[0].length() - 1)));

    }

    @Override
    public String toString() {
        return "<" + operationId + "> " + request.toString();
    }

    @Override
    public boolean equals(Object other) {
        return (other instanceof OperationDescriptor) && this.toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}
