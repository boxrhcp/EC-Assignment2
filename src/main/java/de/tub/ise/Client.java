package de.tub.ise;

import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.io.FileWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Exemplary client for you to try your implementation while developing
 * <p>
 * Seperate class with own main method, it doesn't get referenced anywhere else in
 * the code... You can extend and run the main method to try out different
 * requests
 */
public class Client {

    private static int iterations = 100;

    private final ManagedChannel channel;
    private final KeyValueStoreBlockingStub blockingStub;

    /**
     * Construct client connecting to server at {@code host:port}.
     */
    public Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    /**
     * Construct client for accessing server using the existing channel. Create
     * blockingStub for synchronous communication
     */
    Client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = KeyValueStoreGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Example put request with synchronous gRPC interface {@code blockingStub}.
     */
    public boolean put(String key, String value) {
        //System.out.println("\nWriting data...");
        KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
        Response response;
        try {
            response = blockingStub.put(request);
        } catch (StatusRuntimeException e) {
            return false;
        }

        return response.getSuccess();
    }

    /**
     * Example get request.
     */
    public boolean get(String key) {
        //System.out.println("\nGetting data... ");
        Key request = Key.newBuilder().setKey(key).build();
        Response response;
        try {
            response = blockingStub.get(request);
        } catch (StatusRuntimeException e) {
            return false;
        }

        return response.getSuccess();
    }

    public static void main(String[] args) throws Exception {
        //no need to measure delete, no need to deal with inconsistencies
        String[] nodes = {"nodeA:54.235.226.177:8081", "nodeB:3.120.98.216:8082",
                "nodeC:18.184.24.35:8083"};
        // Change client host and port accordingly

        FileWriter csvWriter = new FileWriter("331-results.csv");
        csvWriter.append("Node access");
        csvWriter.append(",");
        csvWriter.append("Operation");
        csvWriter.append(",");
        csvWriter.append("Max ms");
        csvWriter.append(",");
        csvWriter.append("Min ms");
        csvWriter.append(",");
        csvWriter.append("Average ms");
        csvWriter.append(",");
        csvWriter.append("Failures");
        csvWriter.append(",");
        csvWriter.append("Success %");
        csvWriter.append("\n");
        for (String node : nodes) {
            String[] nodeConf = node.split(":");
            Client client = new Client(nodeConf[1], Integer.parseInt(nodeConf[2]));
            int putFail = 0;
            int getFail = 0;
            try {
                long[] putLat = new long[iterations];
                for (int i = 0; i < iterations; i++) {
                    Instant start = Instant.now();
                    boolean success = client.put("" + i, "Test: " + i);
                    putLat[i] = Duration.between(start, Instant.now()).toMillis();
                    if (!success) putFail++;
                }
                long[] getLat = new long[iterations];
                for (int i = 0; i < iterations; i++) {
                    Instant start = Instant.now();
                    boolean success = client.get("" + i);
                    getLat[i] = Duration.between(start, Instant.now()).toMillis();
                    if (!success) getFail++;
                }
                long maxPut = 0l;
                long maxGet = 0l;
                long minPut = 0l;
                long minGet = 0l;
                long avgPut = 0l;
                long avgGet = 0l;
                for (int i = 0; i < iterations; i++) {
                    long put = putLat[i];
                    long get = getLat[i];
                    if (put > maxPut) maxPut = put;
                    if (put < minPut) minPut = put;
                    if (get > maxGet) maxGet = get;
                    if (get < minGet) minGet = get;
                    avgPut += put;
                    avgGet += get;
                }
                double avPut = avgPut / iterations;
                double avGet = avgGet / iterations;
                double failRatioPut = ((double)(iterations - putFail) / iterations)*100;
                double failRatioGet = ((double)(iterations - getFail) / iterations)*100;
                System.out.println("Results of execution: ");
                System.out.println("PUT - max: " + maxPut + "ms min: "
                        + minPut + "ms avg: "
                        + avPut + "ms");
                System.out.println("GET - max: " + maxGet
                        + "ms min: " + minGet + "ms avg: "
                        + avGet + "ms");
                csvWriter.append(nodeConf[0]);
                csvWriter.append(",");
                csvWriter.append("GET");
                csvWriter.append(",");
                csvWriter.append("" + maxGet);
                csvWriter.append(",");
                csvWriter.append("" + minGet);
                csvWriter.append(",");
                csvWriter.append("" + avGet);
                csvWriter.append(",");
                csvWriter.append("" + getFail);
                csvWriter.append(",");
                csvWriter.append("" + failRatioGet);
                csvWriter.append("\n");
                csvWriter.append(nodeConf[0]);
                csvWriter.append(",");
                csvWriter.append("PUT");
                csvWriter.append(",");
                csvWriter.append("" + maxPut);
                csvWriter.append(",");
                csvWriter.append("" + minPut);
                csvWriter.append(",");
                csvWriter.append("" + avPut);
                csvWriter.append(",");
                csvWriter.append("" + putFail);
                csvWriter.append(",");
                csvWriter.append("" + failRatioPut);
                csvWriter.append("\n");
            } finally {
                client.shutdown();
            }
        }
        csvWriter.flush();
        csvWriter.close();
    }
}