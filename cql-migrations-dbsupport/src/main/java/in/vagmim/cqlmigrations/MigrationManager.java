package in.vagmim.cqlmigrations;


import ch.lambdaj.Lambda;
import com.datastax.driver.core.Session;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import net.sf.cglib.core.CollectionUtils;
import net.sf.cglib.core.Predicate;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

import static ch.lambdaj.Lambda.*;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIn.isIn;


public class MigrationManager
{
    Session session;
    String keyspace;
    SimpleVersionStrategy versionStrategy;
    ResourceMigrationResolver migrationResolver;

    public MigrationManager(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
        versionStrategy = new SimpleVersionStrategy(this.keyspace);
        migrationResolver=new ResourceMigrationResolver();
    }

    public List<Migration> pendingMigrations() throws MigrationException, IOException {
        final List<Long> appliedMigrations = versionStrategy.appliedMigrations(this.session);
        List<Migration> migrations = migrationResolver.resolve();
        List<Migration> pendingMigrations = filter(having(on(Migration.class).getVersion(),
                                                          not(isIn(appliedMigrations))),
                                                   migrations);
        return pendingMigrations;

    }

    public void migrate() throws IOException, MigrationException {
        ensureVersioning();
        List<Migration> migrations = pendingMigrations();
        for(Migration migration: migrations) {
            migration.migrate(this.session,this.keyspace);
            versionStrategy.recordMigration(this.session, migration.getVersion(), DateTime.now());
        }
    }

    private void ensureVersioning() throws MigrationException {
        boolean enabled = versionStrategy.isEnabled(session);
        if(!enabled)
            versionStrategy.enableVersioning(session);
    }
}
