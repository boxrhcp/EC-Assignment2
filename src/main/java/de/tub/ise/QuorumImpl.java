package de.tub.ise;

import java.util.HashMap;

import org.apache.log4j.Logger;
//import de.tub.ise.KeyValuePair;
import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreStub;

import io.grpc.*;

public class QuorumImpl extends KeyValueStoreGrpc.KeyValueStoreImplBase {
    private final int qwritesize;
    private final int qreadsize;
    private final HashMap<String, String> otherNodes;
    private final HashMap<String, KeyValueStoreStub> otherStubs;
    static Logger logger = Logger.getLogger(QuorumImpl.class.getName());

    /**
     * Constructor of Quorum Service
     */
    QuorumImpl() {
        this.qwritesize = KVNodeMain.config.getWriteQuorum();
        this.qreadsize = KVNodeMain.config.getReadQuorum();
        this.otherNodes = KVNodeMain.config.getOtherNodes(KVNodeMain.config.thisNode());
        this.otherStubs = new HashMap<String,KeyValueStoreStub>();
        for (HashMap.Entry<String, String> entry : otherNodes.entrySet()) {
            String node = entry.getKey();
            String host = entry.getValue().split(":")[0];
            int port = Integer.parseInt(entry.getValue().split(":")[1]);
            // TODO create async stubs for communication between nodes
            otherStubs.put(node,KeyValueStoreGrpc.newStub(
                    ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()));
        }

    }

    /**
     * Implementation of put method specified in the .proto file. Handles write
     * requests from the client, produces response with success boolean and key
     * (optional)
     */
    @Override
    public void put(de.tub.ise.KeyValuePair request,
            io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        String key = request.getKey();
        String value = request.getValue();
        Response response;

        logger.debug("Received put request with key " + key);

        if (replicateData(key, value)) {
            response = Response.newBuilder().setSuccess(true).setKey(key).build();
            logger.debug("Telling the client that we replicated");
        } else {
            response = Response.newBuilder().setSuccess(false).setKey(key).build();
            logger.warn("Uh oh, replication not possible :(");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of get method specified in the .proto file. Handles read
     * requests from the client, produces response with success boolean, key and
     * value
     */
    @Override
    public void get(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        String key = request.getKey();
        Response response;

        logger.debug("Received get request with key" + key);

        KeyValuePair data = gatherdata(key);
        if (data == null) {
            response = Response.newBuilder().setSuccess(false).setKey(key).build();
            logger.warn("Uh oh, couldn't get data :(");
        } else {
            response = Response.newBuilder().setSuccess(true).setKey(data.getKey()).setValue(data.getValue()).build();
            logger.debug("Giving client the requested data");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of delete method specified in the .proto file. Handles read
     * requests from the client, produces response with success boolean and original
     * key (optional)
     */
    @Override
    public void delete(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO delete request akin to put request
        String key = request.getKey();
        Response response;
        response = Response.newBuilder().setSuccess(true).setKey(key).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of replicate method specified in the .proto file. You can use
     * this for communication between nodes
     */
    @Override
    public void replicate(de.tub.ise.KeyValuePair request,
            io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle replication requests from other nodes
        String key = request.getKey();
        String value = request.getValue();

        Response response;

        logger.debug("Received replicate request with key " + key);
        Memory.put(key, value);

        response = Response.newBuilder().setSuccess(true).setKey(key).build();
        logger.debug("Telling the node that we replicated");

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of getReplica method specified in the .proto file. You can use
     * this for communication between nodes
     */
    @Override
    public void getReplica(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle request for local replica from other nodes
        String key = request.getKey();
        Response response;

        logger.debug("Received getReplica request with key" + key);

        KeyValuePair data = gatherdata(key);
        if (data == null) {
            response = Response.newBuilder().setSuccess(false).setKey(key).build();
            logger.warn("Uh oh, couldn't get data :(");
        } else {
            response = Response.newBuilder().setSuccess(true).setKey(data.getKey()).setValue(data.getValue()).build();
            logger.debug("Giving node the requested replica data");
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Implementation of deleteReplica method specified in the .proto file. You can
     * use this for communication between nodes
     */
    @Override
    public void deleteReplica(de.tub.ise.Key request,
            io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        // TODO handle delete requests from other nodes, akin to write requests
        String key = request.getKey();
        Response response;
        response = Response.newBuilder().setSuccess(true).setKey(key).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Method to check if quorum replication has been achieved.
     * 
     * You are free to change this method as you see fit
     */
    private boolean replicateData(String key, String value) {

        // Write key-value pair in-memory
        Memory.put(key, value);

        if (qwritesize > 1) {
            // TODO send async. replication requests to nodes
           /* for (HashMap.Entry<String, KeyValueStoreStub> entry : otherStubs.entrySet()) {
                KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
                Response response;
                try {
                    response = entry.getValue().replicate(request);
                } catch (StatusRuntimeException e) {
                    logger.error("Error when sending request to " + entry.getKey());
                    continue;
                }
                System.out.println("Success? " + response.getSuccess());
                if (response.getSuccess()) {
                    System.out.println("Value: " + response.getValue());
                }
            }*/
            // TODO reach write quorum and only then issue client response
            // Timeout after 20s checkear con un while meintras no tengan la repuesta y no pasen 20s
            return false;
        } else
            // TODO send async. replication requests to nodes
            /*for (HashMap.Entry<String, KeyValueStoreStub> entry : otherStubs.entrySet()) {
                KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
                Response response;
                try {
                    response = entry.getValue().put(request);
                } catch (StatusRuntimeException e) {
                    logger.error("Error when sending request to " + entry.getKey());
                    continue;
                }
            }*/
            // But already issue response to client
            return true;
    }

    /**
     * Method to fetch value from memory.
     * 
     * If quorum bigger than 1 is required, fetch replica values from other nodes.
     * You should also check if the returned values from the replicas are
     * consistent.
     * 
     * You are free to change this method if you want
     */
    private KeyValuePair gatherdata(String key) {
        if (qreadsize > 1) {
            // TODO reach read quorum with matching values and issue client response
            // Timeout after 20s
            return null;
        } else {
            // Reads local replica only and issues response
            String data = Memory.get(key);
            if (data == null) {
                logger.warn("Couldn't find data for key " + key);
                return null;
            } else
                return KeyValuePair.newBuilder().setKey(key).setValue(data).build();
        }
    }

    // etc..

}