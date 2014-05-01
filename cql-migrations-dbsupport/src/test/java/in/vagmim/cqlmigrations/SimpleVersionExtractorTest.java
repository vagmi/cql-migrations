package in.vagmim.cqlmigrations;

import in.vagmim.cqlmigrations.exceptions.MigrationException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleVersionExtractorTest {
    @Test
    public void shouldExtractVersion() throws Exception, MigrationException {
        SimpleVersionExtractor versionExtractor = new SimpleVersionExtractor();
        String migrationName = "2014050123324_create_table.cql";
        Long version = versionExtractor.extractVersion(migrationName);
        assertEquals(new Long(2014050123324l),version);
    }
}
