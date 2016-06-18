package ru.ifmo.ctddev.filippov.dkvs;

import java.util.Arrays;

/**
 * Represents proposal value <ballot, slot, command>
 * <p>
 * Created by dimaphil on 03.06.2016.
 */
public class ProposalValue {
    public Ballot ballotNum;
    public int slot;
    OperationDescriptor command;

    ProposalValue(Ballot ballotNum, int slot, OperationDescriptor command) {
        this.ballotNum = ballotNum;
        this.slot = slot;
        this.command = command;
    }

    @Override
    public String toString() {
        return String.format("%s %d %s", ballotNum, slot, command);
    }

    public static ProposalValue parse(String[] parts) {
        return new ProposalValue(Ballot.parse(parts[0]), Integer.parseInt(parts[1]),
                                OperationDescriptor.parse(Arrays.copyOfRange(parts, 2, parts.length)));
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return (other instanceof ProposalValue && this.toString().equals(other.toString()));
    }

}