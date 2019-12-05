package de.tub.ise;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreBlockingStub;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class NodeClient {
    private final ManagedChannel channel;
    private final KeyValueStoreBlockingStub stub;
    static Logger logger = Logger.getLogger(NodeClient.class.getName());


    /** Construct client connecting to server at {@code host:port}. */
    public NodeClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
    }

    /**
     * Construct client for accessing server using the existing channel. Create
     * blockingStub for synchronous communication
     */
    NodeClient(ManagedChannel channel) {
        this.channel = channel;
        stub = KeyValueStoreGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public boolean replicate(String key, String value){
        KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
        Response response;
        try {
            response = stub.replicate(request);
        } catch (StatusRuntimeException e) {
            logger.error("Error when replicating " + key);
            e.printStackTrace();
            return false;
        }
        boolean result = response.getSuccess();
        System.out.println("Success? " + result);

        return result;
    }

    public String getReplica(String key) throws Exception {
        Key request = Key.newBuilder().setKey(key).build();
        Response response;
        try {
            response = stub.getReplica(request);
        } catch (StatusRuntimeException e) {
            throw new Exception("Failed to retrieve replica");
        }
        System.out.println("Success? " + response.getSuccess());
        if (response.getSuccess()) {
            return response.getValue();
        }else{
            throw new Exception("Failed to retrieve replica");
        }
        //return "";
    }

    public boolean deleteReplica(String key){
        Key request = Key.newBuilder().setKey(key).build();
        Response response;
        try {
            response = stub.getReplica(request);
        } catch (StatusRuntimeException e) {
            return false;
        }

        boolean result = response.getSuccess();
        System.out.println("Success? " + result);

        return result;
    }

}
