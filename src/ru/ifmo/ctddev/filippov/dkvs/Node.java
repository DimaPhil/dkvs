package ru.ifmo.ctddev.filippov.dkvs;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

import ru.ifmo.ctddev.filippov.dkvs.messages.*;
import ru.ifmo.ctddev.filippov.dkvs.messages.Message;


/**
 * Represents an instance of a Server (a machine in the system).
 * Should have unique identifier.
 * Created by dimaphil on 03.06.2016.
 */
public class Node implements Runnable, AutoCloseable {
    private int id;

    private ServerSocket inSocket = null;
    static Config mainConfig = null;
    private volatile boolean started = false;
    private volatile boolean stopping = false;

    /**
     * Messages from this queue are polled and handled by handleMessages.
     * Every communication thread puts its received messages into the queue.
     */
    private LinkedBlockingDeque<Message> incomingMessages = new LinkedBlockingDeque<>();

    /**
     * An object for communication between remote Instances through sockets and message queue.
     */
    private class CommunicationEntry {
        Socket input = null;
        Socket output = null;
        LinkedBlockingDeque<Message> messages = new LinkedBlockingDeque<>();

        /**
         * synchronization. retaining messages, connecting
         * and creating output writer should be synchronized.
         */
        volatile boolean ready = false;
        volatile boolean inputAlive = false;
        volatile boolean outputAlive = false;

        void resetOutput() {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException ignored) {
            }
            output = new Socket();
            ready = false;
            messages.retainAll(messages.stream().filter(m ->
                    !(m instanceof Message.PingMessage)).collect(Collectors.toList()));
        }

        void setReady() {
            ready = true;
        }
    }

    private HashMap<Integer, CommunicationEntry> nodes;
    private SortedMap<Integer, CommunicationEntry> clients = new TreeMap<>();

    /**
     * Each node has a Replica, Leader and Acceptor instances.
     */
    private Replica localReplica;
    private Acceptor localAcceptor;
    private Leader localLeader;

    Storage storage;
    Logger logger;
    private Timer timer;

//------------------METHODS----------------------------------------------------

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: Node id");
            System.exit(1);
        }
        int nodeNumber = Integer.parseInt(args[0]);
        new Node(nodeNumber).run();
    }

    private Node(int id) {
        this.id = id;
        storage = new Storage(id);

        try {
            if (mainConfig == null) {
                mainConfig = Config.readPropertiesFile();
            }
            inSocket = new ServerSocket(mainConfig.port(id));
            nodes = new HashMap<>(mainConfig.nodesCount());

            localReplica = new Replica(id, this);
            localLeader = new Leader(id, this);
            localAcceptor = new Acceptor(id, this);

            logger = new Logger(id);

            for (int i = 0; i < mainConfig.nodesCount(); ++i) {
                nodes.put(i, new CommunicationEntry());
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.logError("Node()", e.getMessage());
        }
        timer = new Timer();
    }

    @Override
    public void run() {
        if (started) {
            throw new IllegalStateException("Cannot start a node twice");
        }
        started = true;

        logger.logConnection("run()", "starting node");

        localLeader.startLeader();

        // create output sockets and try to process output messages.
        for (int i = 0; i < mainConfig.nodesCount(); ++i) {
            if (i != id) {
                final int nodeId = i;
                new Thread(() -> speakToNode(nodeId)).start();
            }
        }

        // start processing incoming messages from queue
        new Thread(this::handleMessages).start();

        // listen the server socket and try to accept external connections
        new Thread(() -> {
            while (!stopping) {
                try {
                    Socket client = inSocket.accept();
                    new Thread(() -> handleRequest(client)).start();
                } catch (IOException ignored) {
                }
            }
        }).start();


        TimerTask pingTask = new TimerTask() {
            @Override
            public void run() {
                pingIfIdle();
            }
        };

        TimerTask monitorFaultsTask = new TimerTask() {
            @Override
            public void run() {
                monitorFaults();
            }
        };

        timer.scheduleAtFixedRate(pingTask, mainConfig.timeout, mainConfig.timeout);
        timer.scheduleAtFixedRate(monitorFaultsTask, 4 * mainConfig.timeout, 4 * mainConfig.timeout);
    }

    /**
     * handles the incoming request from specified socket.
     */
    private void handleRequest(Socket client) {
        try {
            InputStreamReader reader = new InputStreamReader(client.getInputStream(), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String msg = bufferedReader.readLine();
            String[] parts = msg.split(" ");

            logger.logMessageIn("handleRequest():", "GOT message [" + msg + "] with request");

            switch (parts[0]) {
                case "node":
                    // (re)connection
                    int nodeId = Integer.parseInt(parts[1]);
                    try {
                        if (nodes.get(nodeId).input != null) {
                            nodes.get(nodeId).input.close();
                        }
                    } catch (IOException ignored) {
                    }
                    nodes.get(nodeId).input = client;

                    logger.logConnection("handleRequest(nodeId:" + nodeId + ")",
                            String.format("#%d: Started listening to node.%d from %s", id, nodeId, client.getInetAddress()));
                    listenToNode(bufferedReader, nodeId);
                    break;
                case "get":
                case "set":
                case "delete":
                    final int newClientId = (clients.keySet().size() == 0) ? 1 :
                            (clients.keySet().stream().max(Comparator.naturalOrder()).get()) + 1;

                    CommunicationEntry entry = new CommunicationEntry();
                    entry.input = client;
                    clients.put(newClientId, entry);

                    // We've already read a message from stream. Now we have to handle it
                    Message firstMessage = ClientRequest.parse(newClientId, parts);
                    sendToNode(id, firstMessage);


                    //Spawn communication thread
                    new Thread(() -> {
                        speakToClient(newClientId);
                    }).start();

                    logger.logConnection("handleRequest(clientId = " + newClientId + ")",
                            String.format("Client %d connected to %d.", newClientId, id));
                    listenToClient(bufferedReader, newClientId);
                    break;
                default:
                    logger.logMessageIn("handleRequest( ... )",
                            "something went wrong: \"" + parts[0] + "\" received");
                    break;
            }

        } catch (IOException e) {
            logger.logError("handleRequest()", e.getMessage());
        }
    }

    /**
     * Takes messages in infinite loop from incoming queue and process them.
     */
    private void handleMessages() {
        while (!stopping) {
            Message message = null;
            try {
                message = incomingMessages.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (message == null) {
                continue;
            }

            logger.logMessageIn("handleMessages()", String.format("Handling message: %s", message));

            if (message instanceof ReplicaMessage) {
                localReplica.receiveMessage((ReplicaMessage) message);
                continue;
            }
            if (message instanceof Message.LeaderMessage) {
                localLeader.receiveMessage((Message.LeaderMessage) message);
                continue;
            }
            if (message instanceof Message.AcceptorMessage) {
                localAcceptor.receiveMessage((Message.AcceptorMessage) message);
                continue;
            }

            logger.logMessageIn("handleMessages()", String.format("Unknown message: %s", message));
        }
    }

    public void close(Map<Integer, CommunicationEntry> map) throws IOException {
        for (CommunicationEntry entry : map.values()) {
            if (entry.input != null) entry.input.close();
            if (entry.output != null) entry.output.close();
        }
    }

    @Override
    public void close() throws Exception {
        stopping = true;
        inSocket.close();
        close(nodes);
        close(clients);
    }

//---------------------SPEAK-&-LISTEN------------------------------------------

    /**
     * A Communication method, it puts all the messages received from
     * another nodes into [incomingMessages]. Should be executed in a separate thread.
     *
     * @param reader is BufferedReader from this node's socket stream.
     * @param nodeId  id of node, which is on the other end of this socket.
     */
    private void listenToNode(BufferedReader reader, int nodeId) {
        nodes.get(nodeId).inputAlive = true;
        while (!stopping) {
            try {
                String data = reader.readLine();
                nodes.get(nodeId).inputAlive = true;
                Message message = Message.parse(nodeId, data.split(" "));

                if (message instanceof Message.PingMessage) {
                    sendToNode(message.getText(), new Message.PongMessage(id));
                    continue;
                }

                if (message instanceof Message.PongMessage) {
                    continue;
                }

                logger.logMessageIn("listenToNode(nodeId:" + nodeId + ")",
                        "GOT message [" + message + "] from " + nodeId);
                sendToNode(id, message);
            } catch (IOException e) {
                logger.logError("listenToNode(nodeId:" + nodeId + ")",
                        nodeId + ": " + e.getMessage());
                break;
            }
        }
    }

    /**
     * A Communication method, it puts all the messages received client
     * into [incomingMessages]. Should be executed in a separate thread.
     */
    private void listenToClient(BufferedReader reader, Integer clientId) {
        logger.logConnection("listenToClient()", String.format("#%d: Client %d connected. Started listening.", id, clientId));
        while (!stopping) {
            try {
                String fromClient = reader.readLine();
                if (fromClient == null)
                    throw new IOException("Client Disconnected.");
                String[] parts = fromClient.split(" ");
                ClientRequest message = ClientRequest.parse(clientId, parts);
                if (message != null) {
                    logger.logMessageIn("listenToClient()",
                            String.format("received message %s from client %d", message, message.getText()));
                    sendToNode(id, message);
                }
            } catch (IOException e) {
                logger.logError("listenToClient()",
                        String.format("Lost connection to Client %d: %s", clientId, e.getMessage()));
                break;
            } catch (IllegalArgumentException e) {
                sendToClient(clientId, new ClientResponse(id, e.getMessage()));
            }
        }
    }

    /**
     * Creates an output socket to the specified node.
     * sends messages from corresponding queue through network to destination using socket.
     */
    private void speakToNode(int nodeId) {
        String address = mainConfig.address(nodeId);
        int port = mainConfig.port(nodeId);

        if (address == null) {
            logger.logError("speakToNode(nodeId" + nodeId + ")",
                    String.format("#%d: Couldn't get address for %d, closing.", id, nodeId));
            return;
        }

        while (!stopping) {
            try {
                nodes.get(nodeId).resetOutput();
                Socket clientSocket = nodes.get(nodeId).output;

                clientSocket.connect(new InetSocketAddress(address, port));
                logger.logConnection("speakToNode(nodeId: " + nodeId + ")",
                        String.format("#%d: CONNECTED to node.%d", id, nodeId));

                sendToNodeAtFirst(nodeId, new Message.NodeMessage(id));

                logger.logMessageOut("speakToNode(nodeId: " + nodeId + ")",
                        String.format("adding node %d to queue for %d", id, nodeId));

                OutputStreamWriter writer = new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8);

                nodes.get(nodeId).setReady();

                while (!stopping) {
                    nodes.get(nodeId).outputAlive = true;
                    Message message = null;
                    try {
                        message = nodes.get(nodeId).messages.takeFirst();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    if (message == null) {
                        continue;
                    }
                    try {
                        writer.write(message + "\n");
                        writer.flush();
                        if(!(message instanceof Message.PingMessage) && !(message instanceof Message.PongMessage))
                        logger.logMessageOut("speakToNode(nodeId: " + nodeId + ")",
                                String.format("SENT to %d: %s", nodeId, message));
                    } catch (IOException ioe) {
                        logger.logError("speakToNode(nodeId: " + nodeId + ")",
                                String.format(
                                        "Couldn't send a message from %d to %d. Retrying.",
                                        id, nodeId));
                        nodes.get(nodeId).messages.addFirst(message);
                        break;
                    }
                }
            } catch (SocketException e) {
                logger.logError("speakToNode(nodeId: " + nodeId + ")",
                        String.format("DISCONNECTION: Connection from %d to node.%d lost: %s",
                                id, nodeId, e.getMessage()));
                try {
                    Thread.sleep(mainConfig.timeout);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            } catch (IOException e) {
                logger.logError("speakToNode(nodeId: " + nodeId + ")",
                        String.format("Connection from %d to node.%d lost: %s",
                                id, nodeId, e.getMessage()));
            }
        }
    }

    /**
     * sends messages from corresponding queue through network to client.
     */
    private void speakToClient(int clientId) {
        try {
            CommunicationEntry entry = clients.get(clientId);
            BlockingDeque<Message> queue = entry.messages;

            OutputStreamWriter writer = new OutputStreamWriter(entry.input.getOutputStream(), StandardCharsets.UTF_8);
            while (!stopping) {
                Message message = null;
                try {
                    message = queue.take();
                } catch (InterruptedException ignored) {
                }
                if (message == null)
                    continue;
                try {
                    logger.logMessageOut("speakToClient(clientId: " + clientId + ")",
                            String.format("#%d: Sending to client %d: %s", id, clientId, message));
                    writer.write(String.format("%s\n", message));
                    writer.flush();
                } catch (IOException ignored) {
                    logger.logMessageOut("speakToClient(clientId: " + clientId + ")",
                            "Couldn't send a message. Retrying.");
                    clients.get(clientId).messages.addFirst(message);
                }
            }
        } catch (IOException ignored) {
        }
    }

    /**
     * Adds given message to the appropriate Nodes's queue.
     */
    void sendToNode(int to, Message message) {
        while (!stopping) {
            try {
                if (to == id) {
                    incomingMessages.put(message);
                } else {
                    nodes.get(to).messages.put(message);
                }
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void sendToNodeAtFirst(int to, Message message) {
        while (!stopping) {
            try {
                if (to == id) {
                    incomingMessages.putFirst(message);
                } else {
                    nodes.get(to).messages.putFirst(message);
                }
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Adds given message to the appropriate Client's queue.
     */
    void sendToClient(int to, Message message) {
        while (!stopping) {
            try {
                clients.get(to).messages.putLast(message);
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Pings nodes, which aren't spoken to last time.
     */
    private void pingIfIdle() {

        nodes.entrySet().stream()
                .filter(node -> (node.getKey() != id))
                .filter(node -> node.getValue().ready)
                .forEach(node -> {
                    if (!node.getValue().outputAlive) {
                        sendToNode(node.getKey(), new Message.PingMessage(id));
                    }
                    node.getValue().outputAlive = false;
                });
    }

    /**
     * Looks for nodes, which didn't respond to us last time.
     */
    private void monitorFaults() {
        HashSet<Integer> faultyNodes = new HashSet<>();
        nodes.entrySet().stream()
                .filter(node -> node.getKey() != id)
                .forEach(node -> {
                    if (!node.getValue().inputAlive) {
                        if (node.getValue().input != null)
                            try {
                                node.getValue().input.close();
                            } catch (IOException ignored) {
                            }
                        faultyNodes.add(node.getKey());
                        logger.logConnection("monitorFaults()", "Node " + node.getKey() + " is faulty, closing its connection.");
                    }
                    node.getValue().inputAlive = false;
                });

        if (faultyNodes.size() > 0) {
            localLeader.notifyFault(faultyNodes);
        }
    }
}

