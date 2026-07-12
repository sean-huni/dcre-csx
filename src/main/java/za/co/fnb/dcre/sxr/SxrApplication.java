package za.co.fnb.dcre.sxr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;
import za.co.fnb.dcre.platform.persistence.JdbcConfig;

@SpringBootApplication
@Import(JdbcConfig.class)
public class SxrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(SxrApplication.class, args);
    }
}
