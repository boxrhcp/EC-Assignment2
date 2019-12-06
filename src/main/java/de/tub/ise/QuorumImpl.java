package de.tub.ise;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import de.tub.ise.KeyValueStoreGrpc.KeyValueStoreStub;
import de.tub.ise.Response;
import io.grpc.stub.StreamObserver;
import io.grpc.*;
import org.apache.log4j.Logger;
//import de.tub.ise.KeyValuePair;


public class QuorumImpl extends KeyValueStoreGrpc.KeyValueStoreImplBase {
    private final int qwritesize;
    private final int qreadsize;
    private final HashMap<String, String> otherNodes;
    private final HashMap<String, KeyValueStoreStub> otherServer;
    static Logger logger = Logger.getLogger(QuorumImpl.class.getName());

    /**
     * Constructor of Quorum Service
     */
    QuorumImpl() {
        this.qwritesize = KVNodeMain.config.getWriteQuorum();
        this.qreadsize = KVNodeMain.config.getReadQuorum();
        this.otherNodes = KVNodeMain.config.getOtherNodes(KVNodeMain.config.thisNode());
        this.otherServer = new HashMap<>();
        for (HashMap.Entry<String, String> entry : otherNodes.entrySet()) {
            String node = entry.getKey();
            String host = entry.getValue().split(":")[0];
            int port = Integer.parseInt(entry.getValue().split(":")[1]);
            // TODO create async stubs for communication between nodes
            logger.info(host + ":" + port);
            otherServer.put(node, KeyValueStoreGrpc.newStub(ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext().build()));
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

        logger.debug("Received get request with key " + key);

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
        // handle replication requests from other nodes
        final String key = request.getKey();
        final String value = request.getValue();
        Response response;

        // Set ctx as the current context within the Runnable
        Context forked = Context.current().fork();
        Context old = forked.attach();
        try {
            logger.debug("Received replicate request with key " + key);
            Memory.put(key, value);
            response = Response.newBuilder().setSuccess(true).setKey(key).build();
            logger.debug("Telling the node that we replicated");
            responseObserver.onNext(response);
        }catch(Exception e){
            responseObserver.onError(e);
        } finally {
            forked.detach(old);
            responseObserver.onCompleted();
        }
    }

    /**
     * Implementation of getReplica method specified in the .proto file. You can use
     * this for communication between nodes
     */
    @Override
    public void getReplica(de.tub.ise.Key request, io.grpc.stub.StreamObserver<de.tub.ise.Response> responseObserver) {
        //handle request for local replica from other nodes
        String key = request.getKey();
        Response response;
        String data = null;
        logger.debug("Received getReplica request with key" + key);

        Context forked = Context.current().fork();
        Context old = forked.attach();
        try {
            data = Memory.get(key);
            if (data == null) {
                response = Response.newBuilder().setSuccess(false).setKey(key).build();
                logger.warn("Uh oh, couldn't get data :(");
            } else {
                response = Response.newBuilder().setSuccess(true).setKey(key).setValue(data).build();
                logger.debug("Giving node the requested replica data for key: " + key);
            }
            responseObserver.onNext(response);
        }catch(Exception e){
            responseObserver.onError(e);
        } finally {
            forked.detach(old);
            responseObserver.onCompleted();
        }
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
        Memory.delete(key);
        Response response;
        response = Response.newBuilder().setSuccess(true).setKey(key).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Method to check if quorum replication has been achieved.
     * <p>
     * You are free to change this method as you see fit
     */
    private boolean replicateData(String key, String value) {
        // Write key-value pair in-memory
        Memory.put(key, value);
        KeyValuePair request = KeyValuePair.newBuilder().setKey(key).setValue(value).build();
        List<Response> results = new ArrayList<Response>();
        final CountDownLatch finishLatch = new CountDownLatch(qwritesize - 1);
        StreamObserver<Response> responses = new StreamObserver<Response>() {
            @Override
            public void onNext(Response response) {
                logger.info("Replica written " + response.getSuccess());
                if (response.getSuccess()) results.add(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Write replica failed: " + t.getMessage());
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                logger.info("Finished replicate");
                finishLatch.countDown();
            }
        };

        // send async. replication requests to nodes
        for (HashMap.Entry<String, KeyValueStoreStub> entry : otherServer.entrySet()) {
            entry.getValue().replicate(request, responses);
        }

        if (qwritesize > 1) {
            try {
                // Wait until minimum quorum reached or Timeout after 20s
                finishLatch.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Quorum timer failed");
            }
            // reach read quorum with matching values and issue client response
            int quorum = 1;
            for (Response result : results) {
                if (result.getSuccess()) {
                    quorum++;
                }
            }
            // reach write quorum and only then issue client response
            if (quorum >= qwritesize) {
                logger.debug("Data replication reached quorum");
                return true;
            } else {
                logger.warn("Data replication failed. Quorum not reached");
                return false;
            }
        } else
            // But already issue response to client
            return true;
    }

    /**
     * Method to fetch value from memory.
     * <p>
     * If quorum bigger than 1 is required, fetch replica values from other nodes.
     * You should also check if the returned values from the replicas are
     * consistent.
     * <p>
     * You are free to change this method if you want
     */
    private KeyValuePair gatherdata(String key) {
        Key request = Key.newBuilder().setKey(key).build();
        List<Response> results = new ArrayList<Response>();
        String data = Memory.get(key);
        if (data == null) {
            logger.warn("Uh oh, couldn't get data for key " + key);
            return null;
        }
        if (qreadsize > 1) {
            final CountDownLatch finishLatch = new CountDownLatch(qreadsize - 1);
            StreamObserver<Response> responses = new StreamObserver<Response>() {
                @Override
                public void onNext(Response response) {
                    logger.info("Received replica " + response.getSuccess());
                    if (response.getSuccess()) results.add(response);
                }

                @Override
                public void onError(Throwable t) {
                    logger.warn("Get Replica failed: " + t.getMessage());
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    logger.info("Finished getReplica");
                    finishLatch.countDown();
                }
            };

            for (HashMap.Entry<String, KeyValueStoreStub> entry : otherServer.entrySet()) {
                entry.getValue().getReplica(request, responses);
            }
            try {
                // Wait until minimum quorum reached or Timeout after 20s
                finishLatch.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Quorum timer failed");
            }
            // TODO reach read quorum with matching values and issue client response
            int quorum = 1;
            for (Response result : results) {
                if (result.getSuccess() && data.equals(result.getValue())) {
                    quorum++;
                }
            }
            if (quorum >= qreadsize) {
                logger.debug("Giving client the requested data");
                return KeyValuePair.newBuilder().setKey(key).setValue(data).build();
            } else {
                logger.warn("Data is inconsistent");
                return null;
            }
        } else {
            // Reads local replica only and issues response
            if (data == null) {
                logger.warn("Couldn't find data for key " + key);
                return null;
            } else
                return KeyValuePair.newBuilder().setKey(key).setValue(data).build();
        }
    }

    // etc..

}