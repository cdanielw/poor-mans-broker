package org.openforis.rmb.examples;

import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.Server;
import org.openforis.rmb.examples.custom.SimpleConnectionManager;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import static org.h2.tools.Server.createTcpServer;

public class Database {
    private static final String SCHEMA = "/example-schema.sql";
    private static final String RESET_SCRIPT = "/example-reset.sql";

    private static final Object LOCK = new Object();
    private static boolean initialized;
    private static JdbcDataSource dataSource;
    private static Server server;
    private static File workingDir = createTempDir();

    public Database() {
        try {
            initDatabase();
        } catch (Exception e) {
            throw new DatabaseException("Failed to create database", e);
        }
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void reset() {
        executeScript(resourceText(RESET_SCRIPT));
    }

    public void stop() {
        server.stop();
        delete(workingDir);
    }

    private void initDatabase() throws SQLException, IOException {
        synchronized (LOCK) {
            if (!initialized) {
                initialized = true;
                int port = findFreePort();
                String url = "jdbc:h2:tcp://localhost:" + port + "/messageBrokerExamples;MODE=PostgreSQL";
                server = createTcpServer("-tcpPort", String.valueOf(port), "-baseDir", workingDir.toString()).start();
                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    public void run() {
                        stop();
                    }
                }));

                dataSource = new JdbcDataSource();
                dataSource.setUrl(url);
                dataSource.setUser("sa");
                dataSource.setPassword("sa");
                setupSchema();
            } else reset();
        }
    }

    private void setupSchema() {
        String schema = resourceText(SCHEMA);
        executeScript(schema);
    }

    private String resourceText(String resource) {
        InputStream is = getClass().getResourceAsStream(resource);
        java.util.Scanner s = new java.util.Scanner(is, "UTF-8").useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private static int findFreePort() throws IOException {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } finally {
            try {
                if (socket != null)
                    socket.close();
            } catch (IOException ignore) {
            }
        }
    }

    @SuppressWarnings({"ResultOfMethodCallIgnored", "ConstantConditions"})
    private void delete(File file) {
        if (file.isDirectory()) {
            for (File c : file.listFiles())
                delete(c);
        }
        file.delete();
    }

    private static File createTempDir() {
        File baseDir = new File(System.getProperty("java.io.tmpdir"));
        String baseName = System.currentTimeMillis() + "-";
        int tempDirAttempts = 10000;
        for (int counter = 0; counter < tempDirAttempts; counter++) {
            File tempDir = new File(baseDir, baseName + counter);
            if (tempDir.mkdir()) {
                return tempDir;
            }
        }
        throw new IllegalStateException("Failed to create directory within "
                + tempDirAttempts + " attempts (tried "
                + baseName + "0 to " + baseName + (tempDirAttempts - 1) + ')');
    }

    private static void executeScript(final String sql) {
        final SimpleConnectionManager connectionManager = new SimpleConnectionManager(dataSource);
        connectionManager.withTransaction(new Callable<Void>() {
            public Void call() throws Exception {
                Connection connection = connectionManager.getCurrentConnection();
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.execute();
                statement.close();
                return null;
            }
        });
    }
}
