package za.co.fnb.dcre.csx.data.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SCRUM-58 (spec 1.4): response_file VARCHAR(128) to VARCHAR(512) so a 129+
 * char reply name no longer crashes the reader insert mid-flow; matches
 * file_arrival.physical_filename(512). The replay-guard
 * UNIQUE (response_file, e2e) from 001-csx.xml must survive the widening.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
class ResponseFileWidthIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    /** 200 chars, realistic reply-name shape (fails on the pre-widening VARCHAR(128)). */
    static final String LONG_NAME = "20260716_FNB_SBSR_%s_RESP.xml"
            .formatted("x".repeat(200 - "20260716_FNB_SBSR__RESP.xml".length()));

    @Autowired
    SbsrRespRepo repo;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void twoHundredCharResponseFileRoundTrips() {
        assertEquals(200, LONG_NAME.length(), "fixture must be exactly 200 chars");

        repo.upsert(LONG_NAME, "MSG-0200", null, "E2E-200", "ACSC", null);

        assertEquals(LONG_NAME, jdbc.queryForObject(
                        "SELECT response_file FROM sbsr_resp WHERE e2e='E2E-200'", String.class),
                "200-char response_file must round-trip byte-exact through sbsr_resp");
    }

    @Test
    void replayGuardUniqueSurvivesWidening() {
        repo.upsert(LONG_NAME, "MSG-0201", null, "E2E-201", "ACSC", null);
        repo.upsert(LONG_NAME, "MSG-0201", null, "E2E-201", "RJCT", "AC04");

        assertEquals(1, jdbc.queryForObject(
                        "SELECT count(*) FROM sbsr_resp WHERE response_file=? AND e2e='E2E-201'",
                        Integer.class, LONG_NAME),
                "replay stays a no-op via ON CONFLICT (response_file, e2e)");
        assertThrows(DuplicateKeyException.class, () -> jdbc.update(
                        "INSERT INTO sbsr_resp (id, response_file, orgnl_msg_id, e2e, status)"
                                + " VALUES (gen_random_uuid(), ?, 'MSG-0201', 'E2E-201', 'ACSC')", LONG_NAME),
                "UNIQUE (response_file, e2e) from 001-csx.xml still enforced after widening");
    }
}
