package in.vagmim.cqlmigrations;


import com.datastax.driver.core.Session;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Migration {


    private String fileName;
    private Long version;
    private Resource resource;
    private SimpleVersionExtractor extractor = new SimpleVersionExtractor();

    public Migration(Resource resource) {
        this.resource = resource;
    }

    public void migrate(Session session, String keyspace) throws IOException {
        session.execute("use " + keyspace + ";");
        List<String> commands = new ArrayList<String>();
        List<String> lines = Files.readAllLines(resource.getFile().toPath(), StandardCharsets.UTF_8);
        StringBuffer buffer = new StringBuffer();
        for(String line : lines) {
            if(line.trim().endsWith(";")){
                buffer.append(line);
                runScript(session,keyspace,buffer.toString());
                buffer = new StringBuffer();
            } else {
                buffer.append(line);
                buffer.append(" ");
            }
        }
        if(!buffer.toString().isEmpty()) {
            runScript(session,keyspace,buffer.toString());
        }
    }
    private void runScript(Session session, String keyspace, String command){
        session.execute("use " + keyspace + ";");
        session.execute(command);
    }

    public String getFileName() {
        return this.resource.getFilename();
    }

    public Long getVersion() throws MigrationException {
        return extractor.extractVersion(this.getFileName());
    }

}
