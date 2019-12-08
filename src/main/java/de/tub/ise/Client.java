package de.tub.ise;

import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Exemplary client for you to try your implementation while developing
 * 
 * Seperate class with own main method, it doesn't get referenced anywhere else in
 * the code... You can extend and run the main method to try out different
 * requests
 */
public class Client {

  private final ManagedChannel channel;
  private final KeyValueStoreBlockingStub blockingStub;

  /** Construct client connecting to server at {@code host:port}. */
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

  /** Example put request with synchronous gRPC interface {@code blockingStub}. */
  public void put(String key, String value) {
    System.out.println("\nWriting data...");
    KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
    Response response;
    try {
      response = blockingStub.put(request);
    } catch (StatusRuntimeException e) {
      return;
    }
    System.out.println("Success? " + response.getSuccess());
  }

  /** Example get request. */
  public void get(String key) {
    System.out.println("\nGetting data... ");
    Key request = Key.newBuilder().setKey(key).build();
    Response response;
    try {
      response = blockingStub.get(request);
    } catch (StatusRuntimeException e) {
      return;
    }
    System.out.println("Success? " + response.getSuccess());
    if (response.getSuccess()) {
      System.out.println("Value: " + response.getValue());
    }
    System.out.println("\n");
  }

  public static void main(String[] args) throws Exception {

    // Change client host and port accordingly
    Client client = new Client("localhost", 8081);

    try {
      long [] putLat = new long [100];
      for(int i = 0; i<100; i++){
        long start = System.nanoTime();
        client.put(""+i, "Test: " + i);
        putLat[i] = System.nanoTime() - start;
      }
      long [] getLat = new long [100];
      for(int i = 0; i<100; i++){
        long start = System.nanoTime();
        client.get(""+i);
        putLat[i] = System.nanoTime() - start;
      }
      long maxPut = 0l;
      long maxGet = 0l;
      long minPut = 0l;
      long minGet = 0l;
      long avgPut = 0l;
      long avgGet = 0l;
      for(int i = 0; i<100; i++){
        long put = putLat[i];
        long get = getLat[i];
        if(put>maxPut) maxPut = put;
        if(put<minPut) minPut = put;
        if(get>maxGet) maxGet = get;
        if(get<minGet) minGet = get;
        avgPut += put;
        avgGet += get;
      }
      double avPut = avgPut/100.0;
      double avGet = avgGet/100.0;
      System.out.println("Results of execution: ");
      System.out.println("PUT - max: " + maxPut + "ns min: "
              + minPut + "ns avg: "
              + avPut + "ns");
      System.out.println("GET - max: " + maxGet 
              + "ns min: " + minGet + "ns avg: "
              + avGet + "ns");


    } finally {
      client.shutdown();
    }

  }
}