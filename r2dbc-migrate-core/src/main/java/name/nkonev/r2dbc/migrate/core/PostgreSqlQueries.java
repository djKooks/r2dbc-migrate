package name.nkonev.r2dbc.migrate.core;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Statement;

import java.util.Arrays;
import java.util.List;
import org.springframework.util.StringUtils;

public class PostgreSqlQueries implements SqlQueries {

    private final String migrationsSchema;
    private final String migrationsTable;
    private final String migrationsLockTable;

    public PostgreSqlQueries(String migrationsSchema, String migrationsTable, String migrationsLockTable) {
        this.migrationsSchema = migrationsSchema;
        this.migrationsTable = migrationsTable;
        this.migrationsLockTable = migrationsLockTable;
    }

    private boolean schemaIsDefined() {
        return !StringUtils.isEmpty(migrationsSchema);
    }

    private String quoteAsObject(String input) {
        return "\"" + input + "\"";
    }

    private String withMigrationsTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsTable));
        } else {
            return String.format(template, quoteAsObject(migrationsTable));
        }
    }

    private String withMigrationsLockTable(String template) {
        if (schemaIsDefined()) {
            return String.format(template, quoteAsObject(migrationsSchema) + "." + quoteAsObject(migrationsLockTable));
        } else {
            return String.format(template, quoteAsObject(migrationsLockTable));
        }
    }

    @Override
    public List<String> createInternalTables() {
        return Arrays.asList(
                withMigrationsTable("create table %s(id int primary key, description text)"),
                withMigrationsLockTable("create table %s(id int primary key, locked boolean not null)"),
                withMigrationsLockTable("insert into %s(id, locked) values (1, false)")
        );
    }

    @Override
    public String getMaxMigration() {
        return withMigrationsTable("select max(id) from %s");
    }

    public String insertMigration() {
        return withMigrationsTable("insert into %s(id, description) values ($1, $2)");
    }

    @Override
    public Statement createInsertMigrationStatement(Connection connection, FilenameParser.MigrationInfo migrationInfo) {
        return connection
                .createStatement(insertMigration())
                .bind("$1", migrationInfo.getVersion())
                .bind("$2", migrationInfo.getDescription());
    }

    @Override
    public String tryAcquireLock() {
        return withMigrationsLockTable("update %s set locked = true where id = 1 and locked = false");
    }

    @Override
    public String releaseLock() {
        return withMigrationsLockTable("update %s set locked = false where id = 1");
    }
}
