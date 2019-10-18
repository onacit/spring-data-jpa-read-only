package com.github.onacit.springdatajpareadonly;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import static java.sql.DriverManager.getConnection;
import static java.sql.DriverManager.setLoginTimeout;
import static org.springframework.util.FileSystemUtils.copyRecursively;
import static org.springframework.util.FileSystemUtils.deleteRecursively;

@SpringBootApplication
@Slf4j
public class SpringDataJpaReadOnlyApplication {

    // -----------------------------------------------------------------------------------------------------------------
    public static final String DB_MASTER = "master";

    public static final int PORT_MASTER = 41521;

    // -----------------------------------------------------------------------------------------------------------------
    public static final String DB_SLAVE = "slave";

    public static final int PORT_SLAVE = 51521;

    // -----------------------------------------------------------------------------------------------------------------
    public static void main(final String[] args) {
        SpringApplication.run(SpringDataJpaReadOnlyApplication.class, args);
    }

    // -----------------------------------------------------------------------------------------------------------------
    @PostConstruct
    private void onPostConstruct() {
        log.debug("onPostConstruct...");
        // -----------------------------------------------------------------------------------------------------------------
        try {
            final Class<?> driverClass = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
            log.debug("driver class loaded: {}", driverClass);
        } catch (final ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        }
        log.info("starting master...");
        try {
            try (Connection c = getConnection("jdbc:derby:" + DB_MASTER + ";create=true")) {
                log.debug("master created: {}", c);
                try (CallableStatement s = c.prepareCall("CALL SYSCS_UTIL.SYSCS_FREEZE_DATABASE()")) {
                    s.execute();
                    log.debug("master froze");
                }
            }
        } catch (final SQLException sqle) {
            throw new RuntimeException(sqle);
        }
        final Path master = new File(".").toPath().resolve(DB_MASTER);
        final Path slave = new File(".").toPath().resolve(DB_SLAVE);
        try {
            deleteRecursively(slave);
        } catch (final IOException ioe) {
            log.error("failed to delete {}", slave, ioe);
        }
        try {
            copyRecursively(master, slave);
            log.debug("copied from {} to {}", master, slave);
        } catch (final IOException ioe) {
            log.error("failed to copy from {} to {}", master, slave, ioe);
        }
        log.info("starting slave...");
        setLoginTimeout(1);
        try {
            try (Connection c = getConnection("jdbc:derby:" + DB_SLAVE + ";startSlave=true")) {
                log.debug("slave connection: {}", c);
            }
            log.debug("slave started");
        } catch (final SQLException sqle) {
            //sqle.printStackTrace();
            //throw new RuntimeException(sqle);
        }
        try {
            try (Connection c = getConnection("jdbc:derby:" + DB_MASTER + ";startMaster=true;slaveHost=localhost")) {
                log.debug("master started: {}", c);
            }
        } catch (final SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }
}
