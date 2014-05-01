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


public class SimpleVersionStrategyTest {

    public static final String TESTKEYSPACE = "testkeyspace";
    public static final String CHECK_KEYSPACE = "select keyspace_name from system.schema_keyspaces where keyspace_name=?";
    private PreparedStatement checkKeySpaceStatement=null;
    private Session session=null;
    private SimpleVersionStrategy simpleVersionStrategy=null;

    @BeforeClass
    public static void setUpCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        System.out.println("Starting embedded cassandra ... ");
        Thread.sleep(7000);
    }

    @Before
    public void setupKeyspace() throws InterruptedException {
        if(session==null) {
            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
            session = cluster.connect();
        }
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        ensureKeySpace(TESTKEYSPACE);
        simpleVersionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
    }

    @After
    public void tearDown() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
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
    private void ensureKeySpace(String testkeyspace) {
        if(!doesKeyspaceExist(testkeyspace)) {
            createKeyspace(testkeyspace);
        }

    }

    private void createKeyspace(String testKeyspace) {
        session.execute("CREATE KEYSPACE " + testKeyspace + " WITH replication " +
                "= {'class':'SimpleStrategy', 'replication_factor':1};");
        session.execute("use " + testKeyspace + ";");
    }

    private boolean doesKeyspaceExist(String testKeyspace) {
        if(checkKeySpaceStatement==null)
            checkKeySpaceStatement = session.prepare(CHECK_KEYSPACE);

        ResultSet rs = session.execute(checkKeySpaceStatement.bind(testKeyspace));
        boolean isExists = rs.all().size() > 0;
        return isExists;
    }
}
