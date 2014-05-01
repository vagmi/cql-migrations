package in.vagmim.cqlmigrations;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MigrationManagerTest extends AbstractCassandraTest{
    @Test
    public void shouldListAllMigrationsAsPendingMigrations() throws Exception, MigrationException {
        SimpleVersionStrategy versionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
        versionStrategy.enableVersioning(session);
        MigrationManager migrationManager = new MigrationManager(session, TESTKEYSPACE);
        List<Migration> migrations = migrationManager.pendingMigrations();
        assertEquals(2,migrations.size());
    }

    @Test
    public void shouldListOneMigrationAsPendingMigrations() throws Exception, MigrationException {
        SimpleVersionStrategy versionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
        versionStrategy.enableVersioning(session);
        versionStrategy.recordMigration(session,20140502111111l, DateTime.now());
        MigrationManager migrationManager = new MigrationManager(session, TESTKEYSPACE);
        List<Migration> migrations = migrationManager.pendingMigrations();
        assertEquals(1,migrations.size());
        assertEquals(new Long(20140501232323l),migrations.get(0).getVersion());
    }

    @Test(expected = InvalidQueryException.class)
    public void shouldMigrateOnlyPendingMigrations() throws IOException, MigrationException {
        SimpleVersionStrategy versionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
        versionStrategy.enableVersioning(session);
        versionStrategy.recordMigration(session,20140502111111l, DateTime.now());
        MigrationManager migrationManager = new MigrationManager(session, TESTKEYSPACE);
        migrationManager.migrate();
        session.execute("select * from customers;");
        session.execute("select * from orders;");
        List<Long> versions = versionStrategy.appliedMigrations(session);
        assertEquals(2,versions.size());
        assertEquals(new Long(20140501232323l),versions.get(0));
        assertEquals(new Long(20140502111111l),versions.get(1));
        session.execute("select * from users;");
    }

    @Test
    public void shouldMigrateAllMigrations() throws IOException, MigrationException {
        MigrationManager migrationManager = new MigrationManager(session, TESTKEYSPACE);
        migrationManager.migrate();
        session.execute("select * from customers;");
        session.execute("select * from orders;");
        session.execute("select * from users;");
        SimpleVersionStrategy versionStrategy = new SimpleVersionStrategy(TESTKEYSPACE);
        List<Long> versions = versionStrategy.appliedMigrations(session);
        assertEquals(2,versions.size());
        assertEquals(new Long(20140501232323l),versions.get(0));
        assertEquals(new Long(20140502111111l),versions.get(1));
    }
}
