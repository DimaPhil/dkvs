package ru.ifmo.ctddev.filippov.dkvs;

import ru.ifmo.ctddev.filippov.dkvs.messages.*;

import java.util.*;

/**
 * Created by dimaphil on 03.06.2016.
 */
class Replica {
    private int id;
    private List<Integer> leaders;

    /**
     * link to the server, where replica is running.
     */
    private Node server;

    /**
     * The index of the next slot in which the replica has not yet proposed any command.
     */
    private volatile int slotIn = 0;

    /**
     * The index of the next slot for which it needs to learn a decision
     * before it can update its copy of the application state, equivalent
     * to the state’s version number (i.e., number of updates).
     */
    private volatile int slotOut = 0;

    /**
     * The replica’s copy of the application state, which we will treat as opaque.
     * All replicas start with the same initial application state.
     */
    private HashMap<String, String> state;

    /**
     * An initially empty set of requests that the replica has received and are not yet proposed or decided.
     */
    private HashSet<OperationDescriptor> requests = new HashSet<>();

    /**
     * An initially empty set of proposals that are currently outstanding.
     */
    private HashMap<Integer, OperationDescriptor> proposals = new HashMap<>();

    /**
     * Another set of proposals that are known to have been decided (also initially empty).
     */
    private HashMap<Integer, OperationDescriptor> decisions = new HashMap<>();

    /**
     * clients not yet responded.
     */
    private HashMap<OperationDescriptor, Integer> awaitingClients = new HashMap<>();
    private HashSet<OperationDescriptor> performed = new HashSet<>();

    Replica(int id, Node server) {
        this.id = id;
        this.server = server;
        this.leaders = Node.mainConfig.ids();

        state = server.storage.kvs;
        slotOut = server.storage.lastSlotOut + 1;
        slotIn = slotOut;
    }

    /**
     * pass to the replica each message, addressed to it.
     *
     * @param message Message that should be handled by the replica.
     */
    void receiveMessage(ReplicaMessage message) {
        if (message instanceof GetRequest) {
            String key = ((GetRequest) message).key;
            String value = state.get(key);
            if (value == null) {
                value = "NOT FOUND";
            } else {
                value = "VALUE " + key + " " + value;
            }

            server.sendToClient(message.getText(), new ClientResponse(message.getText(), value));
            return;
        } else if (message instanceof ClientRequest) {
            OperationDescriptor descriptor = new OperationDescriptor(id, (ClientRequest) message);
            requests.add(descriptor);
            awaitingClients.put(descriptor, message.getText());
        } else if (message instanceof DecisionMessage) {
            DecisionMessage decisionMessage = (DecisionMessage) message;
            OperationDescriptor request = decisionMessage.request;
            int slot = decisionMessage.slot;
            server.logger.logPaxos("Replica.receiveMessage", String.format("DECISION %s", message));
            decisions.put(slot, request);

            while (decisions.containsKey(slotOut)) {
                OperationDescriptor command = decisions.get(slotOut);
                if (proposals.containsKey(slotOut)) {
                    OperationDescriptor proposalCommand = proposals.get(slotOut);
                    proposals.remove(slotOut);

                    if (!command.equals(proposalCommand)) {
                        requests.add(proposalCommand);
                    }
                }
                perform(command);
                slotOut++;
            }
        }
        propose();
    }

    private void propose() {
        while (!requests.isEmpty()) {
            OperationDescriptor descriptor = requests.iterator().next();
            server.logger.logPaxos("Replica.propose", String.format("PROPOSING %s to slot %d", descriptor, slotIn));
            if (!decisions.containsKey(slotIn)) {
                requests.remove(descriptor);
                proposals.put(slotIn, descriptor);
                leaders.forEach(leader -> server.sendToNode(leader, new ProposeMessage(id, slotIn, descriptor)));
            }
            slotIn++;
        }
    }

    private void perform(OperationDescriptor descriptor) {
        server.logger.logPaxos("Replica.perform", String.format("PERFORMING %s at %d", descriptor, slotOut));
        if (performed.contains(descriptor)) {
            //operation was already performed
            return;
        }

        ClientRequest request = descriptor.request;
        if (request instanceof SetRequest) {
            SetRequest setRequest = (SetRequest) request;
            state.put(setRequest.key, setRequest.value);
            Integer awaitingClient = awaitingClients.get(descriptor);
            if (awaitingClient != null) {
                server.sendToClient(awaitingClient, new ClientResponse(request.getText(), "STORED"));
                awaitingClients.remove(descriptor);
            }
        }
        if (request instanceof DeleteRequest) {
            if (performed.contains(descriptor)) {
                return;
            }
            DeleteRequest deleteRequest = (DeleteRequest) request;
            boolean haveKey = state.containsKey(deleteRequest.key);
            state.remove(deleteRequest.key);

            ClientResponse response = new ClientResponse(request.getText(), haveKey ? "DELETED" : "NOT FOUND");

            Integer awaitingClient = awaitingClients.get(descriptor);
            if (awaitingClient != null) {
                server.sendToClient(awaitingClient, response);
                awaitingClients.remove(descriptor);
            }
        }
        performed.add(descriptor);

        if (!(request instanceof GetRequest)) {
            server.storage.saveLog(String.format("slot %d %s", slotOut, descriptor));
        }
    }
}
