package za.co.fnb.dcre.csx.service;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.csx.CrwSourceTables;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-55 Task 7: SBSR replies resolve OrgnlMsgId to the emission batch
 * (crw_emission.outbound_msg_id) once per file; member e2e's correlate via
 * emission_id, foreign e2e's are skipped fail-closed (never ingested), and an
 * unknown OrgnlMsgId ingests fail-open with emission_id NULL (statuses stay
 * truth even if CRW's registry is behind).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
class BatchCorrelationIT {

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

    @Autowired
    ReaderService service;

    @Autowired
    JdbcTemplate jdbc;

    ListAppender<ILoggingEvent> logSink;

    @BeforeEach
    void setUp() {
        CrwSourceTables.create(jdbc);
        logSink = new ListAppender<>();
        logSink.start();
        ((Logger) LoggerFactory.getLogger(ReaderService.class)).addAppender(logSink);
    }

    @AfterEach
    void detachAppender() {
        ((Logger) LoggerFactory.getLogger(ReaderService.class)).detachAppender(logSink);
    }

    @Test
    void memberVerdictsCorrelateToBatchAndForeignE2eIsSkippedFailClosed() {
        String outbound = "DCRERF2026071600000201_2";
        UUID emissionId = seedEmission(outbound, "E2E-A", "E2E-B", "E2E-C");
        String responseFile = "20260716_FNB_SBSR_batch2_RESP.xml";

        int ingested = service.ingest(reply(outbound,
                tx("E2E-A", "ACSC", null), tx("E2E-B", "RJCT", "AC04"), tx("E2E-Z", "ACSC", null)),
                responseFile);

        assertEquals(2, ingested, "foreign e2e Z must not count as ingested");
        assertEquals(2, rowCount(responseFile), "exactly the two member verdicts persisted");
        assertEquals(emissionId, emissionIdOf(responseFile, "E2E-A"));
        assertEquals(emissionId, emissionIdOf(responseFile, "E2E-B"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM sbsr_resp WHERE response_file=?"
                        + " AND e2e='E2E-Z'", Integer.class, responseFile),
                "foreign e2e is NEVER ingested (fail-closed correlation)");
        assertTrue(warnLogged("reason=FOREIGN_E2E", "e2e=E2E-Z", "stage=CSX"),
                "skip must be visible: WARN excluded stage=CSX ... e2e=E2E-Z reason=FOREIGN_E2E");
    }

    @Test
    void unknownOutboundMsgIngestsFailOpenWithNullEmissionId() {
        String responseFile = "20260716_FNB_SBSR_unknown_RESP.xml";

        int ingested = service.ingest(reply("DCRERF2026071600000999",
                tx("E2E-A", "ACSC", null), tx("E2E-B", "PDNG", null)), responseFile);

        assertEquals(2, ingested, "unknown OrgnlMsgId still ingests every verdict (fail-open)");
        assertEquals(2, rowCount(responseFile));
        assertNull(emissionIdOf(responseFile, "E2E-A"), "no batch resolved: emission_id stays NULL");
        assertNull(emissionIdOf(responseFile, "E2E-B"));
        assertEquals(1, logSink.list.stream().map(ILoggingEvent::getFormattedMessage)
                        .filter(m -> m.contains("reason=UNKNOWN_OUTBOUND_MSG")).count(),
                "single file-level WARN for the unknown OrgnlMsgId");
    }

    private UUID seedEmission(final String outboundMsgId, final String... memberE2e) {
        UUID emissionId = UUID.randomUUID();
        jdbc.update("INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state,"
                        + " batch_ordinal, outbound_msg_id) VALUES (?, ?, ?, ?, 'VISIBLE', 2, ?)",
                emissionId, UUID.randomUUID(), LocalDate.of(2026, 7, 16),
                "FNBRF01_%s_PAIN008.xml".formatted(outboundMsgId), outboundMsgId);
        for (int i = 0; i < memberE2e.length; i++) {
            jdbc.update("INSERT INTO crw_emission_member (emission_id, sequence, e2e, amount)"
                    + " VALUES (?, ?, ?, 100.00)", emissionId, i + 1, memberE2e[i]);
        }
        return emissionId;
    }

    // [SYNTHETIC-CONTRACT R-35] reply shape
    private static String reply(final String orgnlMsgId, final String... txBlocks) {
        StringBuilder xml = new StringBuilder("<Document>\n  <OrgnlMsgId>")
                .append(orgnlMsgId).append("</OrgnlMsgId>\n");
        List.of(txBlocks).forEach(block -> xml.append("  ").append(block).append('\n'));
        return xml.append("</Document>\n").toString();
    }

    private static String tx(final String e2e, final String status, final String reason) {
        return "<Tx><OrgnlEndToEndId>%s</OrgnlEndToEndId><TxSts>%s</TxSts>%s</Tx>"
                .formatted(e2e, status, reason == null ? "" : "<Rsn>%s</Rsn>".formatted(reason));
    }

    private int rowCount(final String responseFile) {
        return jdbc.queryForObject("SELECT count(*) FROM sbsr_resp WHERE response_file=?",
                Integer.class, responseFile);
    }

    private UUID emissionIdOf(final String responseFile, final String e2e) {
        return jdbc.queryForObject("SELECT emission_id FROM sbsr_resp WHERE response_file=? AND e2e=?",
                UUID.class, responseFile, e2e);
    }

    private boolean warnLogged(final String... fragments) {
        return logSink.list.stream().map(ILoggingEvent::getFormattedMessage)
                .anyMatch(m -> List.of(fragments).stream().allMatch(m::contains));
    }
}
