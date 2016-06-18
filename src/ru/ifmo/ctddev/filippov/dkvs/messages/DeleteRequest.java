package ru.ifmo.ctddev.filippov.dkvs.messages;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class DeleteRequest extends ClientRequest {
    //int fromId;
    public String key;


    public DeleteRequest(int fromId, String key) {
        this.fromId = fromId;
        this.key = key;
    }

    @Override
    public String toString() {
        return String.format("delete %d %s", fromId, key);
    }


    @Override
    public boolean equals(Object other) {
        if (other instanceof GetRequest) {
            if (this.toString().equals(other.toString()))
                return true;
        }
        return false;
    }

}
