
package worker.recovery;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * Saves coordinator recovery snapshots to a local checkpoint file.
 */
public class CoordinatorStateStore {

    public record TaskSnapshot(
            String jobId,
            String taskId,
            String status,
            int assignedWorkerId,
            boolean hasResult,
            long resultValue) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    public record Snapshot(
            int coordinatorId,
            int term,
            int jac,
            List<TaskSnapshot> tasks) implements Serializable {
        private static final long serialVersionUID = 1L;

        public Snapshot {
            tasks = List.copyOf(tasks);
        }
    }

    private final Path checkpointFile;

    public CoordinatorStateStore(Path checkpointFile) {
        if (checkpointFile == null) {
            throw new IllegalArgumentException(
                    "Checkpoint path cannot be null");
        }

        this.checkpointFile = checkpointFile.toAbsolutePath();
    }

    /**
     * Writes a complete snapshot to a temporary file before replacing
     * the previous checkpoint.
     */
    public synchronized void save(Snapshot snapshot)
            throws IOException {

        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Snapshot cannot be null");
        }

        Path parent = checkpointFile.getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path temporaryFile = checkpointFile.resolveSibling(
                checkpointFile.getFileName() + ".tmp");

        try {
            try (ObjectOutputStream output =
                         new ObjectOutputStream(
                                 Files.newOutputStream(temporaryFile))) {
                output.writeObject(snapshot);
                output.flush();
            }

            try {
                Files.move(
                        temporaryFile,
                        checkpointFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException exception) {
                Files.move(
                        temporaryFile,
                        checkpointFile,
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporaryFile);
        }
    }

    /**
     * Returns the saved snapshot, or null if no checkpoint exists.
     */
    public synchronized Snapshot load() throws IOException {
        if (!Files.exists(checkpointFile)) {
            return null;
        }

        try (ObjectInputStream input =
                     new ObjectInputStream(
                             Files.newInputStream(checkpointFile))) {

            Object value = input.readObject();

            if (!(value instanceof Snapshot snapshot)) {
                throw new IOException(
                        "Checkpoint contains an unexpected object");
            }

            return snapshot;
        } catch (ClassNotFoundException exception) {
            throw new IOException(
                    "Unable to read checkpoint data", exception);
        }
    }

    public synchronized boolean exists() {
        return Files.exists(checkpointFile);
    }

    public synchronized void delete() throws IOException {
        Files.deleteIfExists(checkpointFile);
    }
}
