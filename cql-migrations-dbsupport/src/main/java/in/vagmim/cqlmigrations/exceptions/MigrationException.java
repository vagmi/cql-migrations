package in.vagmim.cqlmigrations.exceptions;

public class MigrationException extends Throwable {
    public MigrationException(String s, Exception e) {
        super(s,e);
    }

    public MigrationException(Exception e) {
        super(e);
    }
}
