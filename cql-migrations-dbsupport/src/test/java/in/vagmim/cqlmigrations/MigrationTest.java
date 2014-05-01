package in.vagmim.cqlmigrations;

import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import static org.junit.Assert.assertEquals;


public class MigrationTest extends AbstractCassandraTest {
    Migration migration;
    @Before
    public void setupMigration() {
        Resource migrationResource = new ClassPathResource("cql/migrations/20140501232323_create_test_tables.cql");
        migration = new Migration(migrationResource);
    }

    @Test
    public void shouldMigrate() throws Exception {
        migration.migrate(session,TESTKEYSPACE);
        session.execute("use " + TESTKEYSPACE + ";");
        session.execute("select * from customers;");
        session.execute("select * from orders;");
    }

    @Test
    public void shouldGetFileName() throws Exception {
       assertEquals("20140501232323_create_test_tables.cql",  migration.getFileName());
    }

    @Test
    public void shouldGetVersion() throws Exception, MigrationException {
        assertEquals(new Long(20140501232323l),  migration.getVersion());
    }
}
