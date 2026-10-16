package client.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Opens a gRPC channel to host:port.
 *
 * Production uses plain TCP (plaintext(), as in the lab). Tests supply an
 * in-process version, so the real client code runs against fake servers
 * without opening network ports.
 */
@FunctionalInterface
public interface ChannelFactory {

    ManagedChannel open(String host, int port);

    /** Plaintext TCP channel, as used in the gRPC labs. */
    static ChannelFactory plaintext() {
        return (host, port) -> ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }
}
