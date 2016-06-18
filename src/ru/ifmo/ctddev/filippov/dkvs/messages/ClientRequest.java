package ru.ifmo.ctddev.filippov.dkvs.messages;

/**
 * Created by dimaphil on 03.06.2016.
 */
public abstract class ClientRequest extends ReplicaMessage {
    public static ClientRequest parse(int clientId, String[] parts) throws IllegalArgumentException {
        if (parts.length < 2)
            throw new IllegalArgumentException("Unknown client request");
        switch (parts[0]) {
            case "get":
                return new GetRequest(clientId, parts[1]);
            case "set":
                if (parts.length < 3)
                    throw new IllegalArgumentException("Incorrect SET request");
                return new SetRequest(clientId, parts[1], parts[2]);
            case "delete":
                return new DeleteRequest(clientId, parts[1]);
            default:
                throw new IllegalArgumentException("Unknown client request");
        }
    }
}
