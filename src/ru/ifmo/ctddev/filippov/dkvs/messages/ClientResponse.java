package ru.ifmo.ctddev.filippov.dkvs.messages;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class ClientResponse extends Message {
    private String data;

    public ClientResponse(int fromId, String data) {
        this.fromId = fromId;
        this.data = data;
    }

    @Override
    public String toString() {
        return data;
    }
}

