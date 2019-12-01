package de.tub.ise;

import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

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
    Client client = new Client("localhost", 8082);

    try {
      client.put("123", "Maria Borges, TU Berlin");
      client.get("123");
    } finally {
      client.shutdown();
    }

  }
}