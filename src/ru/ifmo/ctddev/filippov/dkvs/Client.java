package ru.ifmo.ctddev.filippov.dkvs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Created by dimaphil on 05.06.2016.
 */
public class Client {
    public static void main(String[] args) throws IOException {

        int id = 0;
        if(args.length != 0) {
            id = Integer.parseInt(args[0]);
        }

        Config config = Config.readPropertiesFile();
        int[] ports = config.ports();
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            int port = ports[id];
            Socket socket = new Socket();
            InetSocketAddress address = new InetSocketAddress("localhost", port);
            socket.connect(address);
            System.out.println("Connected: " + port);

            OutputStreamWriter socketWriter = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
            InputStreamReader socketReader = new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(socketReader);

            while (true) {
                String request = reader.readLine();
                System.out.println("Request: " + request);
                if (request == null) {
                    socketWriter.close();
                    return;
                }

                socketWriter.write(request + "\n");
                socketWriter.flush();

                String response = bufferedReader.readLine();
                System.out.println("Response: " + response);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
