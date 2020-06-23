package name.nkonev.r2dbc.migrate.core;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static name.nkonev.r2dbc.migrate.core.TestConstants.waitTestcontainersSeconds;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.shaded.com.google.common.util.concurrent.Uninterruptibles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MssqlTestcontainersConcurrentStartTest {

    final static int MSSQL_HARDCODED_PORT = 3333;

    static volatile GenericContainer container;
    final static String password = "yourStrong(!)Password";

    private static final Logger LOGGER = LoggerFactory.getLogger(MssqlTestcontainersConcurrentStartTest.class);

    static final Random random = new Random();

    private Mono<Connection> makeConnectionMono(int port) {
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "mssql")
                .option(HOST, "127.0.0.1")
                .option(PORT, port)
                .option(USER, "sa")
                .option(PASSWORD, password)
                .option(DATABASE, "master")
                .build());
        ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
            .maxIdleTime(Duration.ofSeconds(8))
            .acquireRetry(10)
            .maxAcquireTime(Duration.ofSeconds(4))
            .maxSize(20)
            .build();

        ConnectionPool pool = new ConnectionPool(configuration);
        Publisher<? extends Connection> connectionPublisher = pool.create();
        return Mono.from(connectionPublisher);
    }

    static class Client {
        String firstName, secondName, account;
        int estimatedMoney;

        public Client(String firstName, String secondName, String account, int estimatedMoney) {
            this.firstName = firstName;
            this.secondName = secondName;
            this.account = account;
            this.estimatedMoney = estimatedMoney;
        }
    }

    @RepeatedTest(50)
    public void testThatMigratorCanHandleSituationWhenDatabaseStillStarting() {
        int randomInteger = random.nextInt(10);
        Thread thread = new Thread(() -> {
            LOGGER.info("Sleeping random {} seconds before start the container", randomInteger);
            Uninterruptibles.sleepUninterruptibly(randomInteger, TimeUnit.SECONDS);
            container = new GenericContainer("mcr.microsoft.com/mssql/server:2017-CU20-ubuntu-16.04")
                .withExposedPorts(MSSQL_HARDCODED_PORT)
                .withEnv("ACCEPT_EULA", "Y")
                .withEnv("SA_PASSWORD", password)
                .withEnv("MSSQL_COLLATION", "cyrillic_general_ci_as")
                .withEnv("MSSQL_TCP_PORT", ""+MSSQL_HARDCODED_PORT)
                .waitingFor(new LogMessageWaitStrategy().withRegEx(".*The default collation was successfully changed.*\\s")
                    .withStartupTimeout(Duration.ofSeconds(waitTestcontainersSeconds)));
            container.setPortBindings(Arrays.asList(MSSQL_HARDCODED_PORT+":"+MSSQL_HARDCODED_PORT));
            container.start();
        });
        thread.setDaemon(true);
        thread.start();

        try {
            R2dbcMigrateProperties properties = new R2dbcMigrateProperties();
            properties.setConnectionMaxRetries(1024);
            properties.setDialect(Dialect.MSSQL);
            properties.setResourcesPath("classpath:/migrations/mssql/*.sql");
            R2dbcMigrate.migrate(() -> makeConnectionMono(MSSQL_HARDCODED_PORT), properties)
                .block();

            Flux<Client> clientFlux = makeConnectionMono(MSSQL_HARDCODED_PORT)
                .flatMapMany(connection -> Flux.from(
                    connection.createStatement("select * from sales_department.rich_clients.client")
                        .execute()).doFinally(signalType -> connection.close()))
                .flatMap(o -> o.map((row, rowMetadata) -> {
                    return new Client(
                        row.get("first_name", String.class),
                        row.get("second_name", String.class),
                        row.get("account", String.class),
                        row.get("estimated_money", Integer.class)
                    );
                }));
            Client client = clientFlux.blockLast();

            Assertions.assertEquals("John", client.firstName);
            Assertions.assertEquals("Smith", client.secondName);
            Assertions.assertEquals("4444", client.account);
            Assertions.assertEquals(9999999, client.estimatedMoney);
        } finally {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (container!=null) {
                container.stop();
            }

        }
    }
}
