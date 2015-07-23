package org.openforis.rmb.spring

import groovy.sql.Sql
import org.h2.jdbcx.JdbcDataSource
import org.h2.tools.Server

import javax.sql.DataSource

import static org.h2.tools.Server.createTcpServer

class ExampleDatabase {
    private static final String SCHEMA = '/example-schema.sql'
    private static final String RESET_SCRIPT = '/example-reset.sql'

    private static final Object LOCK = new Object()
    private static boolean initialized
    private static DataSource dataSource
    private static Server server
    private static File workingDir = File.createTempDir()

    private String url

    ExampleDatabase() {
        initDatabase()
    }

    DataSource getDataSource() { dataSource }

    void reset() {
        new Sql(dataSource).execute(resourceText(RESET_SCRIPT))
    }

    private void initDatabase() {
        synchronized (LOCK) {
            if (!initialized) {
                initialized = true
                def port = findFreePort()
                url = "jdbc:h2:tcp://localhost:$port/messageRepository;MODE=PostgreSQL"
                server = createTcpServer("-tcpPort $port -baseDir $workingDir".split(' ')).start()
                addShutdownHook {
                    stop()
                }

                dataSource = new JdbcDataSource(url: url,
                        user: 'sa', password: 'sa')
                setupSchema()
            } else reset()
        }
    }

    private void setupSchema() {
        def schema = resourceText(SCHEMA)
        new Sql(dataSource).execute(schema)
    }

    private String resourceText(String resource) {
        getClass().getResourceAsStream(resource).getText('UTF-8')
    }

    void stop() {
        server.stop()
        workingDir.deleteDir()
    }

    static int findFreePort() {
        ServerSocket socket = null
        try {
            socket = new ServerSocket(0);
            return socket.localPort
        } finally {
            try {
                socket?.close()
            } catch (IOException ignore) {
            }
        }
    }
}
