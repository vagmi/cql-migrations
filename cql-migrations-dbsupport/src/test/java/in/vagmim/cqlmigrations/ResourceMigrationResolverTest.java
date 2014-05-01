package in.vagmim.cqlmigrations;

import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;


public class ResourceMigrationResolverTest {
    @Test
    public void shouldResolveMigrations() throws Exception, MigrationException {
        ResourceMigrationResolver resolver = new ResourceMigrationResolver();
        List<Migration> migrations = resolver.resolve();
        assertEquals(2,migrations.size());
        assertEquals(new Long(20140501232323l),migrations.get(0).getVersion());
        assertEquals(new Long(20140502111111l),migrations.get(1).getVersion());
    }
}
