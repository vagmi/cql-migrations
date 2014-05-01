package in.vagmim.cqlmigrations;

import ch.lambdaj.function.convert.Converter;
import in.vagmim.cqlmigrations.exceptions.MigrationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static ch.lambdaj.Lambda.convert;
import static ch.lambdaj.Lambda.on;
import static ch.lambdaj.Lambda.sort;

public class ResourceMigrationResolver
{
    private static final String CLASSPATH_MIGRATIONS_SQL = "classpath:/cql/migrations/*";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private String migrationsLocation;
    private SimpleVersionExtractor versionExtractor;

    public ResourceMigrationResolver()
    {
        this(CLASSPATH_MIGRATIONS_SQL);
    }

    public ResourceMigrationResolver(String migrationsLocation)
    {
        this(migrationsLocation, new SimpleVersionExtractor());
    }

    public ResourceMigrationResolver(String migrationsLocation, SimpleVersionExtractor versionExtractor)
    {
        setMigrationsLocation(migrationsLocation);
        setVersionExtractor(versionExtractor);
    }
    public List<Migration> resolve() throws IOException, MigrationException {
        PathMatchingResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        List<Resource> resources = Arrays.asList(patternResolver.getResources(this.migrationsLocation));
        List<Migration> migrations = convert(resources, new Converter<Resource, Migration>() {
            @Override
            public Migration convert(Resource res) {
                return new Migration(res);
            }
        });
        List<Migration> sortedMigrations = sort(migrations, on(Migration.class).getVersion());
        return sortedMigrations;
    }


    public void setMigrationsLocation(String migrationsLocation)
    {
        this.migrationsLocation = migrationsLocation;
    }

    public void setVersionExtractor(SimpleVersionExtractor versionExtractor)
    {
        this.versionExtractor = versionExtractor;
    }
}
