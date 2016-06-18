package ru.ifmo.ctddev.filippov.dkvs.messages;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class SetRequest extends ClientRequest {
    public String key;
    public String value;

    SetRequest(int fromId, String key, String value) {
        this.fromId = fromId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("set %d %s %s", fromId, key, value);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof SetRequest) {
            if (this.toString().equals(other.toString()))
                return true;
        }
        return false;
    }
}
