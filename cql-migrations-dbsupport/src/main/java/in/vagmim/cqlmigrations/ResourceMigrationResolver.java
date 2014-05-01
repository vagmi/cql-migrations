package in.vagmim.cqlmigrations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

public class ResourceMigrationResolver
{
//    private static final String CLASSPATH_MIGRATIONS_SQL = "classpath:/cql/migrations/";
//
//    protected final Logger logger = LoggerFactory.getLogger(getClass());
//
//    private String migrationsLocation;
//    private SimpleVersionExtractor versionExtractor;
//
//    public ResourceMigrationResolver()
//    {
//        this(CLASSPATH_MIGRATIONS_SQL);
//    }
//
//    public ResourceMigrationResolver(String migrationsLocation)
//    {
//        this(migrationsLocation, new SimpleVersionExtractor());
//    }
//
//    public ResourceMigrationResolver(String migrationsLocation, SimpleVersionExtractor versionExtractor)
//    {
//        setMigrationsLocation(migrationsLocation);
//        setVersionExtractor(versionExtractor);
//    }
//
//    public Set<Migration> resolve(DatabaseType dbType)
//    {
//        Set<Migration> migrations = new HashSet<Migration>();
//
//        // Find all resources in the migrations location.
//        String convertedMigrationsLocation = convertMigrationsLocation(migrationsLocation, dbType);
//
//        PathMatchingResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
//        List<Resource> resources;
//        try
//        {
//            resources = new ArrayList<Resource>(Arrays.asList(patternResolver.getResources(convertedMigrationsLocation)));
//        }
//        catch (IOException e)
//        {
//            throw new MigrationException(e);
//        }
//
//        // Remove resources starting with a '.' (e.g. .svn, .cvs, etc)
//        CollectionUtils.filter(resources, new Predicate()
//        {
//            public boolean evaluate(Object object)
//            {
//                try
//                {
//                    return (((Resource) object).isReadable() && !((Resource) object).getFilename().startsWith("."));
//                }
//                catch (Exception e)
//                {
//                    if (logger.isDebugEnabled())
//                    {
//                        logger.debug("Exception while filtering resource.", e);
//                    }
//                    return false;
//                }
//            }
//        });
//
//        if (resources.isEmpty())
//        {
//            String message = "No migrations were found using resource pattern '" + migrationsLocation + "'.";
//            logger.error(message);
//            throw new MigrationException(message);
//        }
//
//        if (logger.isDebugEnabled())
//        {
//            logger.debug("Found " + resources.size() + " resources: " + collectionToCommaDelimitedString(resources));
//        }
//
//        // Extract versions and create executable migrations for each resource.
//        for (Resource resource : resources)
//        {
//            String version = versionExtractor.extractVersion(resource.getFilename());
//            if (find(migrations, new Migration.MigrationVersionPredicate(version)) != null)
//            {
//                String message = "Non-unique migration version.";
//                logger.error(message);
//                throw new MigrationException(message);
//            }
//            migrations.add(migrationFactory.create(version, resource));
//        }
//
//        return migrations;
//    }
//
//    public Set<Migration> resolve()
//    {
//        return resolve(DatabaseType.UNKNOWN);
//    }
//
//    protected String convertMigrationsLocation(String migrationsLocation, DatabaseType dbType)
//    {
//        String converted = migrationsLocation;
//
//        if (!(isBlank(FilenameUtils.getName(converted)) || FilenameUtils.getName(converted).contains("*")))
//        {
//            converted += "/";
//        }
//
//        if (!FilenameUtils.getName(converted).contains("*"))
//        {
//            converted += "*";
//        }
//
//        if (!(converted.startsWith("file:") || converted.startsWith("classpath:")))
//        {
//            converted = "file:" + converted;
//        }
//
//        return converted;
//    }
//
//    public void setMigrationsLocation(String migrationsLocation)
//    {
//        this.migrationsLocation = migrationsLocation;
//    }
//
//    public void setVersionExtractor(SimpleVersionExtractor versionExtractor)
//    {
//        this.versionExtractor = versionExtractor;
//    }
//
//    public void setMigrationFactory(MigrationFactory migrationFactory)
//    {
//        this.migrationFactory = migrationFactory;
//    }
}
