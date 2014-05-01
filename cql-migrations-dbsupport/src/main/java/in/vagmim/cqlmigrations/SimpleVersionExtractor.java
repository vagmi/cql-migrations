package in.vagmim.cqlmigrations;

import in.vagmim.cqlmigrations.exceptions.MigrationException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SimpleVersionExtractor
{
    private static final Pattern FILENAME_PATTERN = Pattern.compile("^(\\d+).*");

    public Long extractVersion(String name) throws MigrationException {
        try
        {
            Matcher matcher = FILENAME_PATTERN.matcher(name);
            matcher.find();
            return Long.parseLong(matcher.group(1));
        }
        catch (Exception e)
        {
            throw new MigrationException("Error parsing migration version from " + name, e);
        }
    }
}
