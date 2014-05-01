package in.vagmim.cqlmigrations;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.joda.time.DateTime;
import org.junit.*;
import sun.print.resources.serviceui_sv;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class SimpleVersionStrategyTest extends AbstractCassandraTest {

    SimpleVersionStrategy simpleVersionStrategy;
    @Before
    public void setupVersionStrategy() {
        simpleVersionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
    }
    @Test
    public void shouldNotBeEnabledWhenSchemaVersionsIsNotFound() throws Exception {
        boolean enabled = simpleVersionStrategy.isEnabled(session);
        assertFalse(enabled);
    }
    @Test
    public void shouldBeEnabledWhenSchemaVersionsIsNotFound() throws Exception {
        session.execute("create table schema_versions(ksname text, version bigint, applied_on timestamp, primary key (ksname, version));");
        boolean enabled = simpleVersionStrategy.isEnabled(session);
        assertTrue(enabled);
    }

    @Test
    public void enableVersioningShouldCreateSchemaVersionsTable() throws Exception, MigrationException {
        boolean statusBeforeEnabling = simpleVersionStrategy.isEnabled(session);
        simpleVersionStrategy.enableVersioning(session);
        boolean statusAfterEnabling = simpleVersionStrategy.isEnabled(session);
        assertFalse(statusBeforeEnabling);
        assertTrue(statusAfterEnabling);
    }

    @Test
    public void shouldReturnAppliedMigrations() throws Exception, MigrationException {
        simpleVersionStrategy.enableVersioning(session);
        simpleVersionStrategy.recordMigration(session, 1l, DateTime.now());
        simpleVersionStrategy.recordMigration(session, 2l, DateTime.now());
        List<Long> versions = simpleVersionStrategy.appliedMigrations(session);
        assertEquals(2,versions.size());
        assertEquals(new Long(1l),versions.get(0));
        assertEquals(new Long(2l),versions.get(1));
    }

    @Test
    public void shouldRecordNewMigration() throws Exception, MigrationException {
        simpleVersionStrategy.enableVersioning(session);
        simpleVersionStrategy.recordMigration(session, 1l, DateTime.now());
        simpleVersionStrategy.recordMigration(session, 2l, DateTime.now());
        ResultSet rs = session.execute("select ksname,version from schema_versions where ksname='" + TESTKEYSPACE+"';");
        assertEquals(2,rs.all().size());
    }

}
