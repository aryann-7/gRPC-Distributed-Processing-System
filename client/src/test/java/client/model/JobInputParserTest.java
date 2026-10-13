package client.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

public class JobInputParserTest {

    @Test
    public void parsesCommasSpacesAndNewlines() {
        assertEquals(List.of(10L, 25L, 3L, 99L), JobInputParser.parseNumbers("10, 25 3,\n99"));
    }

    @Test
    public void rejectsBadTokenAndNamesIt() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> JobInputParser.parseNumbers("1, 2, abc, 4"));
        assertTrue(e.getMessage().contains("abc"));
    }

    @Test
    public void rejectsEmptyInput() {
        assertThrows(IllegalArgumentException.class, () -> JobInputParser.parseNumbers("   "));
    }

    @Test
    public void buildsRangeJobForPrimesum() {
        JobSpec job = JobInputParser.build(JobKind.PRIMESUM, "ignored", "1", "20");
        assertEquals(JobKind.PRIMESUM, job.kind());
        assertEquals(1, job.rangeStart());
        assertEquals(20, job.rangeEnd());
        assertTrue(job.jobId().startsWith("job-"));
    }

    @Test
    public void rejectsReversedRange() {
        assertThrows(IllegalArgumentException.class,
                () -> JobInputParser.build(JobKind.PRIMESUM, "", "50", "10"));
    }

    @Test
    public void buildsListJobForMax() {
        JobSpec job = JobInputParser.build(JobKind.MAX, "5, 9, 2", "", "");
        assertEquals(List.of(5L, 9L, 2L), job.numbers());
    }

    @Test
    public void validatesPort() {
        assertEquals(1099, JobInputParser.parsePort(" 1099 "));
        assertThrows(IllegalArgumentException.class, () -> JobInputParser.parsePort("70000"));
        assertThrows(IllegalArgumentException.class, () -> JobInputParser.parsePort("abc"));
    }
}
