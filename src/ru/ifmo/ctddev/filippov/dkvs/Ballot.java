package ru.ifmo.ctddev.filippov.dkvs;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class Ballot implements Comparable<Ballot> {
    int ballotNum;
    int leaderId;

    Ballot(int ballotNum, int leaderId) {
        this.ballotNum = ballotNum;
        this.leaderId = leaderId;
    }

    boolean lessThan(Ballot other) {
        return this.compareTo(other) < 0;
    }

    @Override
    public int compareTo(Ballot other) {
        int result = new Integer(ballotNum).compareTo(other.ballotNum);
        if (result == 0) {
            result = new Integer(other.leaderId).compareTo(leaderId);
        }
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Ballot &&
                (ballotNum == ((Ballot) other).ballotNum) && (leaderId == ((Ballot) other).leaderId);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        return String.format("%d_%d", ballotNum, leaderId);
    }

    public static Ballot parse(String ballotRepr) {
        String[] parts = ballotRepr.split("_");
        return new Ballot(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }
}
