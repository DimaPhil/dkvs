package ru.ifmo.ctddev.filippov.dkvs.messages;

import java.util.Arrays;

import ru.ifmo.ctddev.filippov.dkvs.OperationDescriptor;
import ru.ifmo.ctddev.filippov.dkvs.Ballot;
import ru.ifmo.ctddev.filippov.dkvs.ProposalValue;

/**
 * Messages between Servers and Clients. Abstract message class.
 * Created by dimaphil on 03.06.2016.
 */
public abstract class Message {
    protected String text;
    int fromId;

    public int getText() {
        return fromId;
    }

    public static class PingMessage extends Message {
        public PingMessage(int fromId) {
            this.fromId = fromId;
        }

        @Override
        public String toString() {
            return "ping from " + fromId;
        }
    }

    public static class PongMessage extends Message {
        public PongMessage(int fromId) {
            this.fromId = fromId;
        }

        @Override
        public String toString() {
            return "pong";
        }
    }

    public static class NodeMessage extends Message {
        public NodeMessage(int fromId) {
            this.fromId = fromId;
        }

        @Override
        public String toString() {
            return String.format("node %d", fromId);
        }
    }

    public static class LeaderMessage extends Message {
        LeaderMessage(int fromId) {
            this.fromId = fromId;
        }
    }

    public static abstract class AcceptorMessage extends Message {

        public Ballot ballotNum;

        AcceptorMessage(int fromId, Ballot ballotNum) {
            this.ballotNum = ballotNum;
            this.fromId = fromId;
        }
    }

    public static class P1Request extends AcceptorMessage {
        public P1Request(int fromId, Ballot ballotNum) {
            super(fromId, ballotNum);
        }

        @Override
        public String toString() {
            return String.format("p1a %d %s", fromId, ballotNum);
        }
    }

    public static class P2Request extends AcceptorMessage {
        public ProposalValue payload;

        public P2Request(int fromId, ProposalValue payload) {
            super(fromId, payload.ballotNum);
            this.payload = payload;
        }

        @Override
        public String toString() {
            return String.format("p2a %d %s", fromId, payload);
        }
    }

    public static Message parse(int fromId, String[] parts) {
        switch (parts[0]) {
            case "node":
                return new NodeMessage(Integer.parseInt(parts[1]));
            case "ping":
                return new PingMessage(fromId);
            case "pong":
                return new PongMessage(fromId);
            case "decision":
                return new DecisionMessage(Integer.parseInt(parts[1]),
                        OperationDescriptor.parse(Arrays.copyOfRange(parts, 2, parts.length)));
            case "propose":
                return new ProposeMessage(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]),
                        OperationDescriptor.parse(Arrays.copyOfRange(parts, 3, parts.length)));
            case "p1a":
                return new P1Request(Integer.parseInt(parts[1]), Ballot.parse(parts[2]));
            case "p2a":
                return new P2Request(Integer.parseInt(parts[1]),
                        ProposalValue.parse(Arrays.copyOfRange(parts, 2, parts.length)));
            case "p1b":
                return P1Response.parse(parts);
            case "p2b":
                return new P2Responce(Integer.parseInt(parts[1]), Ballot.parse(parts[2]),
                        ProposalValue.parse(Arrays.copyOfRange(parts, 3, parts.length)));
            default:
                throw new IllegalArgumentException("Unknown message.");
        }
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }
}



