package za.co.fnb.dcre.csx.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.util.UUID;

/** One reply verdict per original transaction (SBSR response leg). */
@Table("sbsr_resp")
public class SbsrRespEntity extends BaseEntity {

    private String responseFile;
    private String orgnlMsgId;
    /** SCRUM-55: emission batch the verdict correlates to; NULL when unresolved. */
    private UUID emissionId;
    private String e2e;
    private String status;
    private String reason;

    public String getResponseFile() { return responseFile; }
    public String getOrgnlMsgId() { return orgnlMsgId; }
    public UUID getEmissionId() { return emissionId; }
    public String getE2e() { return e2e; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
}
