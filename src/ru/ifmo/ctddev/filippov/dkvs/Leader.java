package ru.ifmo.ctddev.filippov.dkvs;

import ru.ifmo.ctddev.filippov.dkvs.messages.*;

import java.util.*;

/**
 * Created by dimaphil on 03.06.2016.
 */
public class Leader {
    private int id;
    private Node machine;
    private List<Integer> replicas;
    private List<Integer> acceptors;
    private volatile boolean isActive;
    private volatile Ballot currentBallot;

    /**
     * A map of slot numbers to proposed commands in the form of a set of
     * (slot number, command) pairs, initially empty. At any time, there is
     * at most one entry per slot number in the set).
     */
    private HashMap<Integer, OperationDescriptor> proposals;
    private HashMap<ProposalValue, Commander> commanders;
    private HashMap<Ballot, Scout> scouts;
    private int timeToFault = -1;

    private class Scout {
        HashSet<Integer> waitFor;
        HashMap<Integer, ProposalValue> proposals;
        Ballot ballot;

        Scout(Ballot ballot) {
            this.ballot = ballot;
            waitFor = new HashSet<>(acceptors);
            proposals = new HashMap<>();
        }

        void receiveResponse(P1Response response) {
            if (response.ballotNum.equals(ballot)) {
                response.pvalues.forEach(pvalue -> {
                    if (!proposals.containsKey(pvalue.slot) || proposals.get(pvalue.slot).ballotNum.lessThan(pvalue.ballotNum)) {
                        proposals.put(pvalue.slot, pvalue);
                    }
                });
                waitFor.remove(response.getText());

                if (waitFor.size() < (acceptors.size() + 1) / 2) {
                    adopted(ballot, proposals);
                }
            } else {
                preempted(response.ballotNum);
            }
        }
    }

    private class Commander {
        ProposalValue proposal;
        HashSet<Integer> waitFor;

        Commander(ProposalValue proposal) {
            this.proposal = proposal;
            this.waitFor = new HashSet<>(acceptors);
        }

        void receiveResponse(P2Responce response) {
            if (response.ballot.equals(currentBallot)) {
                waitFor.remove(response.getText());
                if (waitFor.size() < (acceptors.size() + 1) / 2) {
                    replicas.forEach(replica ->
                            machine.sendToNode(replica, new DecisionMessage(response.proposal.slot, response.proposal.command))
                    );
                }
            } else {
                preempted(response.ballot);
            }
        }
    }

    Leader(int id, Node machine) {
        this.id = id;
        this.machine = machine;
        this.acceptors = Node.mainConfig.ids();
        this.replicas = Node.mainConfig.ids();
        proposals = new HashMap<>();
        currentBallot = new Ballot(machine.storage.lastBallot, id);
        isActive = (id == 0);

        commanders = new HashMap<>();
        scouts = new HashMap<>();
    }

    void startLeader() {
        startScouting(currentBallot);
    }

    void receiveMessage(Message.LeaderMessage message) {
        machine.logger.logPaxos("Leader.receiveMessage", "pushed message [" + message + "]");
        if (message instanceof ProposeMessage) {
            ProposeMessage proposeMessage = (ProposeMessage) message;
            if (!proposals.containsKey(proposeMessage.slot)) {
                proposals.put(proposeMessage.slot, proposeMessage.request);
                if (isActive) {
                    command(new ProposalValue(currentBallot, proposeMessage.slot, proposeMessage.request));
                } else {
                    machine.logger.logPaxos("Leader.receiveMessage", "LEADER " + id + " IS NOT ACTIVE SINCE NOW.");
                }
            } else {
                machine.logger.logError("Leader.receiveMessage", "slot " +
                        proposeMessage.slot + " already used!");
            }
        }
        if (message instanceof P1Response) {
            P1Response response = (P1Response) message;
            Ballot ballot = response.originalBallot;
            Scout scout = scouts.get(ballot);
            scout.receiveResponse(response);
        }
        if (message instanceof P2Responce) {
            P2Responce response = (P2Responce) message;
            ProposalValue proposal = response.proposal;
            Commander commander = commanders.get(proposal);
            commander.receiveResponse(response);
        }
    }

    /**
     * Sent by either a scout or a commander, it means that some acceptor has adopted (r', L').
     * If (r', L') > ballot_num, it may no longer be possible
     * to use ballot ballot_num to choose a command.
     */
    private void preempted(Ballot b) {
        machine.logger.logPaxos("PREEMPTED: ballot started - " + b);
        if (b.compareTo(currentBallot) > 0) {
            isActive = false;
            machine.logger.logPaxos(String.format("LEADER %d IS NOT ACTIVE SINCE NOW!", id));
            machine.logger.logPaxos(String.format("WAITING for leader %d to fail", b.leaderId));
            timeToFault = b.leaderId;
            currentBallot = new Ballot(machine.storage.nextBallot(), id);
            machine.storage.saveLog("ballot " + currentBallot);
        }
    }

    /**
     * Sent by a scout,this message signiﬁes that the current ballot number
     * ballot_num has been adopted by a majority of acceptors.
     * (If an adopted message arrives for an old ballot number, it is ignored [by Scout].)
     * The set pvalues contains all pvalues accepted by these acceptors prior to ballot_num.
     */
    private void adopted(Ballot ballot, Map<Integer, ProposalValue> pvalues) {
        machine.logger.logPaxos(String.format("ADOPTED, ballot = %s", ballot));

        for (Map.Entry<Integer, ProposalValue> proposalValue : pvalues.entrySet()) {
            Integer key = proposalValue.getKey();
            ProposalValue value = proposalValue.getValue();
            proposals.put(key, value.command);
        }
        isActive = true;

        for (Map.Entry<Integer, OperationDescriptor> proposal : proposals.entrySet()) {
            Integer key = proposal.getKey();
            OperationDescriptor value = proposal.getValue();
            command(new ProposalValue(ballot, key, value));
        }
    }

    private void command(ProposalValue proposal) {
        machine.logger.logPaxos(String.format("COMMANDER started for %s", proposal));
        commanders.put(proposal, new Commander(proposal));
        acceptors.forEach(a -> machine.sendToNode(a, new Message.P2Request(id, proposal)));
    }

    void notifyFault(HashSet<Integer> faultTimes) {
        if (!isActive && faultTimes.contains(timeToFault)) {
            startScouting(currentBallot);
        }
    }

    private void startScouting(Ballot ballot) {
        scouts.put(ballot, new Scout(currentBallot));
        acceptors.forEach(a -> machine.sendToNode(a, new Message.P1Request(id, ballot)));
    }
}



