package client.api;

/**
 * Creates a CoordinatorClient for a given discovery address
 * (the bootstrap / registry host and port typed into the GUI).
 *
 * ClientMain decides which transport to use; the GUI only calls create().
 */
@FunctionalInterface
public interface CoordinatorClientFactory {
    CoordinatorClient create(String host, int port);
}
