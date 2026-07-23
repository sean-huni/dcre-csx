package za.co.fnb.dcre.sxr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;
import za.co.fnb.dcre.platform.batch.config.BatchJdbcConfig;
import za.co.fnb.dcre.platform.batch.config.HeartbeatDatasourceConfig;
import za.co.fnb.dcre.platform.persistence.JdbcConfig;

@SpringBootApplication
@Import({JdbcConfig.class, BatchJdbcConfig.class, HeartbeatDatasourceConfig.class})
public class SxrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(SxrApplication.class, args);
    }
}
