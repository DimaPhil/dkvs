package ru.ifmo.ctddev.filippov.dkvs;

import ru.ifmo.ctddev.filippov.dkvs.messages.*;
import ru.ifmo.ctddev.filippov.dkvs.messages.P1Response;

import java.util.HashMap;

/**
 * An acceptor is passive and only sends messages in response to requests.
 * It runs in an infinite loop, receiving two kinds of request messages from leaders.
 *
 * Created by dimaphil on 03.06.2016.
 */
class Acceptor {
    private int id;
    private volatile Ballot ballotNumber;
    private Node server;
    private HashMap<Integer, ProposalValue> accepted;

    Acceptor(int id, Node server) {
        this.id = id;
        this.server = server;
        this.ballotNumber = new Ballot(server.storage.lastBallot - 1, Node.mainConfig.ids().get(0));
        this.accepted = new HashMap<>();
    }

    void receiveMessage(Message.AcceptorMessage message) {
        if (message instanceof Message.P1Request) {
            if (ballotNumber.lessThan(message.ballotNum)) {
                ballotNumber = message.ballotNum;
                server.logger.logPaxos("Acceptor.receiveMessage: " + id, "ACCEPTOR ADOPTED " + ballotNumber);
            }
            server.sendToNode(message.getText(),
                    new P1Response(id, message.ballotNum, ballotNumber, accepted.values()));
            return;
        }
        if (message instanceof Message.P2Request) {
            Message.P2Request request = (Message.P2Request) message;
            if (request.payload.ballotNum.equals(ballotNumber)) {
                accepted.put(request.payload.slot, request.payload);
                server.logger.logPaxos("Acceptor.receiveMessage: " + id, "ACCEPTOR ACCEPTED " + ballotNumber);
            }
            server.sendToNode(message.getText(), new P2Responce(id, ballotNumber, request.payload));
            return;
        }
        throw new IllegalArgumentException("Incorrect message");
    }
}


