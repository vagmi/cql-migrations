package in.vagmim.cqlmigrations.exceptions;

import com.datastax.driver.core.exceptions.InvalidQueryException;

public class MigrationException extends Throwable {
    public MigrationException(String s, InvalidQueryException e) {
        super(s,e);
    }

    public MigrationException(InvalidQueryException e) {
        super(e);
    }
}
