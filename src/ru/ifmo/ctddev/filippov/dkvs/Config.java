package ru.ifmo.ctddev.filippov.dkvs;

import java.io.*;
import java.util.*;

/**
 * Created by dimaphil on 03.06.2016.
 */
class Config {
    private Map<Integer, String> addresses;
    long timeout;

    private Config(Map<Integer, String> map, int timeout) {
        this.addresses = map;
        this.timeout = timeout;
    }

    String address(int id) {
        if (!addresses.containsKey(id)) {
            return null;
        }
        return addresses.get(id).split(":")[0];
    }

    int port(int id) {
        if (!addresses.containsKey(id)) {
            return -1;
        }
        return Integer.parseInt(addresses.get(id).split(":")[1]);
    }

    int[] ports() {
        int nodes = nodesCount();
        int[] result = new int[nodes];
        for (int i = 0; i < nodes; i++) {
            result[i] = port(i);
        }
        return result;
    }

    int nodesCount() {
        return addresses.size();
    }

    private static List<Integer> range(int min, int max) {
        List<Integer> list = new ArrayList<>();
        for (int i = min; i <= max; i++) {
            list.add(i);
        }
        return list;
    }

    List<Integer> ids() {
        return range(0, nodesCount() - 1);
    }

    static Config readPropertiesFile() throws IOException {
        String FILENAME = "src/dkvs.properties";
        String PREFIX = "node";

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(FILENAME)));
        HashMap<Integer, String> ids = new HashMap<>();
        int timeout = 1000;
        while (true) {
            try {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.startsWith(PREFIX)) {
                    int id = Integer.parseInt(line.split("=")[0].split("\\.")[1].trim());
                    String port = line.split("=")[1].trim();
                    //System.out.println("Id, port = " + id + " " + port);
                    ids.put(id, port);
                }
                if (line.startsWith("timeout")) {
                    timeout = Integer.parseInt(line.split("=")[1].trim());
                    //System.out.println("Timeout = " + timeout);
                }
            } catch (IOException ignored) {
                break;
            }
        }
        return new Config(Collections.unmodifiableMap(ids), timeout);
    }
}


