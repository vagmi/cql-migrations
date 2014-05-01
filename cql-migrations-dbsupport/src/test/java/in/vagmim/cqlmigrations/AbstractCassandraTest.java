package in.vagmim.cqlmigrations;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class AbstractCassandraTest {
    public static final String TESTKEYSPACE = "testkeyspace";
    public static final String CHECK_KEYSPACE = "select keyspace_name from system.schema_keyspaces where keyspace_name=?";
    protected Session session=null;
    private PreparedStatement checkKeySpaceStatement=null;

    @BeforeClass
    public static void setUpCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        System.out.println("Starting embedded cassandra ... ");
        Thread.sleep(1000);
    }

    @Before
    public void setupKeyspace() throws InterruptedException {
        if(session==null) {
            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
            session = cluster.connect();
        }
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        ensureKeySpace(TESTKEYSPACE);
    }

    @After
    public void tearDown() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
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
