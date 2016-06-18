package ru.ifmo.ctddev.filippov.dkvs;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dimaphil on 04.06.2016.
 *
 * This class is used to store pairs <key, value> of our dkvs.
 * Each replica has a link to the Storage (Node has a link and replica has a link to the Node) so that is can operate with it.
 * Also this class provides functionality to operate with log file: to save the state and to restore it
 */
class Storage {
    private String filename;
    private BufferedWriter writer = null;

    volatile int lastBallot = 0;
    volatile HashMap<String, String> kvs;
    volatile int lastSlotOut = -1;

    Storage(int id) {
        filename = String.format("dkvs_%d.log", id);
        try {
            writer = new BufferedWriter(new FileWriter(filename, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        kvs = new HashMap<>();

        BufferedReader reader = null;
        try {
            File file = new File(filename);
            if (!file.exists() && !file.createNewFile()) {
                throw new IOException("Can't create file " + filename);
            }
            reader = new BufferedReader(new FileReader(file));
        } catch (IOException e) {
            e.printStackTrace();
        }

        ArrayList<String> lines = new ArrayList<>();
        assert reader != null;
        lines.addAll(reader.lines().collect(Collectors.toList()));
        Collections.reverse(lines);

        HashMap<String, String> kvsNew = new HashMap<>();
        HashSet<String> removed = new HashSet<>();

        LOOP:
        for (String line : lines) {
            String[] tokens = line.split(" ");
            switch (tokens[0]) {
                case "ballot":
                    lastBallot = Math.max(lastBallot, Ballot.parse(tokens[1]).ballotNum);
                    break;
                case "slot":
                    String key = tokens.length >= 6 ? tokens[5] : null;
                    lastSlotOut = Math.max(lastSlotOut, Integer.parseInt(tokens[1]));
                    if (kvsNew.containsKey(key) || removed.contains(key)) {
                        continue LOOP;
                    }
                    switch (tokens[3]) {
                        case "set":
                            kvsNew.put(key, tokens[6]);
                            break;
                        case "delete":
                            removed.add(key);
                            break;
                    }
                    break;
                default:
                    throw new AssertionError("Something went wrong, unexpected token at Storage: " + tokens[0]);
            }
        }

        kvs = kvsNew;

        for (String line : lines) {
            String[] tokens = line.split(" ");
            if (tokens[0].equals("ballot")) {
                lastBallot = Ballot.parse(tokens[1]).ballotNum;
                break;
            }
        }
    }

    void saveLog(String s) {
        try {
            writer.write(s);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.out.println("Can't write to file");
            e.printStackTrace();
            System.exit(1);
        }
    }

    int nextBallot() {
        return ++lastBallot;
    }
}
