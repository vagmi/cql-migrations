package in.vagmim.cqlmigrations;

import ch.lambdaj.Lambda;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static ch.lambdaj.Lambda.on;

public class SimpleVersionStrategy {
    public static final String SELECT_VERSION_FROM_SCHEMA_VERSION = "select version from schema_versions where ksname=?";
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private static final String CREATE_SCHEMA_VERSIONS = "create table schema_versions (ksname text, version bigint, applied_on timestamp, primary key (ksname,version));";
    private static final String RECORD_MIGRATION = "insert into schema_versions(ksname,version,applied_on) values (?,?,?)";
    private PreparedStatement recordMigrationStatement = null;
    private PreparedStatement selectVersionsStatement = null;
    private String keyspaceName;

    public SimpleVersionStrategy(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }


    public boolean isEnabled(Session session) {
        try {
            preparedSelectVersionsStatement(session);
            session.execute(selectVersionsStatement.bind(this.keyspaceName));
        } catch (InvalidQueryException e) {
            return false;
        }
        return true;
    }

    public void enableVersioning(Session session) throws MigrationException {
        try {
            session.execute(CREATE_SCHEMA_VERSIONS);
        } catch (InvalidQueryException e) {
            throw new MigrationException("Could not create version-tracking table.", e);
        }
    }

    public List<Long> appliedMigrations(Session session) throws MigrationException {
        // Make sure migrations is enabled.
        if (!isEnabled(session))
            return null;
        List<Long> versions;
        try {
            preparedSelectVersionsStatement(session);
            ResultSet rs = session.execute(selectVersionsStatement.bind(this.keyspaceName));
            List<Row> rows = rs.all();
            versions = Lambda.extract(rows, on(Row.class).getLong("version"));
        } catch (InvalidQueryException e) {
            throw new MigrationException(e);
        }
        return versions;
    }

    private void preparedSelectVersionsStatement(Session session) {
        if(selectVersionsStatement==null)
            selectVersionsStatement = session.prepare(SELECT_VERSION_FROM_SCHEMA_VERSION);
    }

    public void recordMigration(Session session, Long version, DateTime startTime) throws MigrationException {
        try {
            if (recordMigrationStatement == null)
                recordMigrationStatement = session.prepare(RECORD_MIGRATION);
            BoundStatement boundStatement = recordMigrationStatement.bind(this.keyspaceName, version, startTime.toDate());
            session.execute(boundStatement);
        } catch (InvalidQueryException e) {
            throw new MigrationException(e);
        }
    }
}
