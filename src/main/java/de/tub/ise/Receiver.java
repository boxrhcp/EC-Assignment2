package de.tub.ise;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 * Receiver class, i.e. gRPC Server that serves the KeyValueStore service (see
 * kvstore.proto).
 */
public class Receiver {

    static Logger logger = Logger.getLogger(Receiver.class.getName());

    private final Server server;

    public Receiver() throws IOException {
        this.server = ServerBuilder.forPort(KVNodeMain.config.getReceivePort()).addService(new QuorumImpl()).build();
    }

    /** Start serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + KVNodeMain.config.getReceivePort());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                Receiver.this.stop();
                System.err.println("*** server shut down");
            }
        });

    }

    /** Stop serving requests and shutdown resources. */
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon
     * threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
