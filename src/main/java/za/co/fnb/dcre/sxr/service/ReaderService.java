package za.co.fnb.dcre.sxr.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import za.co.fnb.dcre.sxr.data.repo.SbsrRespRepo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Business tier: parses the reply file and upserts one row per Tx block.
 * Replay of the same file is a no-op via
 * ON CONFLICT (response_file, e2e).
 *
 * [SYNTHETIC-CONTRACT R-35] Reply shape: one OrgnlMsgId element, then
 * repeated Tx blocks of OrgnlEndToEndId + TxSts with an optional Rsn.
 *
 * <p>SCRUM-42 load fix: a 300k-row reply ingested in ONE serializable
 * transaction is unrefreshable; CRDB aborts it with RETRY_SERIALIZABLE
 * "can't refresh txn spans" and the step-level retry just re-runs the same
 * doomed giant transaction (IXR died exit 5 on the 300k sweep; SBSR replies
 * are the same shape). Upserts therefore commit in bounded slices. Committed
 * slices stand when a later slice fails: the upsert targets the row's
 * business identity (response_file, e2e), so a restart (step-level retry or
 * job relaunch) no-ops over them and resumes the rest.
 *
 * <p>SCRUM-55 batch correlation: OrgnlMsgId resolves once per file to the
 * emission batch (crw_emission.outbound_msg_id). A member e2e persists with
 * that emission_id; a foreign e2e is skipped fail-closed (WARN, never
 * ingested); an unknown OrgnlMsgId ingests fail-open with emission_id NULL
 * (statuses are still truth even if CRW's registry is behind).
 */
@Service
public class ReaderService {

    private static final Logger log = LoggerFactory.getLogger(ReaderService.class);

    private static final Pattern ORGNL_MSG_ID =
            Pattern.compile("<OrgnlMsgId>([^<]+)</OrgnlMsgId>");
    private static final Pattern TX = Pattern.compile(
            "<Tx>\\s*<OrgnlEndToEndId>([^<]+)</OrgnlEndToEndId>"
                    + "\\s*<TxSts>([^<]+)</TxSts>(?:\\s*<Rsn>([^<]+)</Rsn>)?",
            Pattern.DOTALL);

    private record Verdict(String e2e, String status, String reason) {
    }

    private final SbsrRespRepo repo;
    private final TransactionTemplate sliceTx;
    private final int sliceSize;

    public ReaderService(final SbsrRespRepo repo, final PlatformTransactionManager txManager,
                         @Value("${dcre.sxr.ingest-slice-size:10000}") final int sliceSize) {
        this.repo = repo;
        // Each slice commits in its OWN transaction so a 300k-row reply
        // ratchets progress slice by slice; and a CRDB 40001 abort poisons the
        // surrounding transaction (25P02 on any further statement), so a retry
        // needs a fresh transaction per attempt.
        this.sliceTx = new TransactionTemplate(txManager);
        this.sliceTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.sliceSize = sliceSize;
    }

    /** @return number of Tx verdicts ingested (foreign e2e skips excluded). */
    public int ingest(final String fileText, final String responseFile) {
        Matcher msgId = ORGNL_MSG_ID.matcher(fileText);
        if (!msgId.find()) {
            throw new IllegalArgumentException("reply file has no <OrgnlMsgId>: " + responseFile);
        }
        String orgnlMsgId = msgId.group(1);
        Optional<UUID> emissionId = repo.findEmissionIdByOutboundMsgId(orgnlMsgId);
        if (emissionId.isEmpty()) {
            log.warn("excluded stage=SXR arrival=- seq=-1 e2e=- reason=UNKNOWN_OUTBOUND_MSG"
                    + " orgnlMsgId={} file={}", orgnlMsgId, responseFile);
        }
        Set<String> memberE2e = emissionId.map(repo::findMemberE2e).map(HashSet::new).orElse(null);
        List<Verdict> verdicts = parse(fileText);
        int ingested = 0;
        for (int from = 0; from < verdicts.size(); from += sliceSize) {
            ingested += writeSlice(responseFile, orgnlMsgId, emissionId.orElse(null), memberE2e,
                    verdicts.subList(from, Math.min(from + sliceSize, verdicts.size())), from);
        }
        return ingested;
    }

    /** Single pass over the ~40MB reply string; parsing stays out of the write transactions. */
    private List<Verdict> parse(final String fileText) {
        Matcher tx = TX.matcher(fileText);
        List<Verdict> verdicts = new ArrayList<>();
        while (tx.find()) {
            verdicts.add(new Verdict(tx.group(1), tx.group(2), tx.group(3)));
        }
        return verdicts;
    }

    /** One slice = one committed unit: fresh REQUIRES_NEW tx per bounded-retry attempt. */
    private int writeSlice(final String responseFile, final String orgnlMsgId,
                           final UUID emissionId, final Set<String> memberE2e,
                           final List<Verdict> slice, final int from) {
        return CrdbRetry.run("ingest slice file=%s from=%d".formatted(responseFile, from),
                () -> sliceTx.execute(status -> {
                    int ingested = 0;
                    for (final Verdict verdict : slice) {
                        if (memberE2e != null && !memberE2e.contains(verdict.e2e())) {
                            log.warn("excluded stage=SXR arrival=- seq=-1 e2e={}"
                                    + " reason=FOREIGN_E2E file={}", verdict.e2e(), responseFile);
                            continue;
                        }
                        repo.upsert(responseFile, orgnlMsgId, emissionId,
                                verdict.e2e(), verdict.status(), verdict.reason());
                        ingested++;
                    }
                    return ingested;
                }));
    }
}
