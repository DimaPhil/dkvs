package ru.ifmo.ctddev.filippov.dkvs.messages;

import ru.ifmo.ctddev.filippov.dkvs.Ballot;
import ru.ifmo.ctddev.filippov.dkvs.ProposalValue;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class P2Responce extends Message.LeaderMessage {
    public Ballot ballot;
    public ProposalValue proposal;

    public P2Responce(int fromId, Ballot ballot, ProposalValue proposal) {
        super(fromId);
        this.ballot = ballot;
        this.proposal = proposal;
    }

    @Override
    public String toString() {
        return String.format("p2b %d %s %s", fromId, ballot, proposal);
    }
}
