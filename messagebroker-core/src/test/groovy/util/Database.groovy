package util

import groovy.sql.Sql
import org.h2.jdbcx.JdbcDataSource
import org.h2.tools.Server
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.sql.DataSource

import static org.h2.tools.Server.createTcpServer

class Database {
    private static final Logger LOG = LoggerFactory.getLogger(this)
    private static final String SCHEMA = '/schema-postgres.sql'
    private static final String RESET_SCRIPT = '/reset.sql'

    private static final Object LOCK = new Object()
    private static boolean initialized
    private static DataSource dataSource
    private static Server server
    private static File workingDir = File.createTempDir()


    Database() {
        initDatabase()
    }

    DataSource getDataSource() { dataSource }

    void reset() {
        long time = System.currentTimeMillis()
        new Sql(dataSource).execute(resourceText(RESET_SCRIPT))
        LOG.info("Reset database in ${System.currentTimeMillis() - time} millis.")
    }

    private void initDatabase() {
        synchronized (LOCK) {
            if (!initialized) {
                initialized = true
                long time = System.currentTimeMillis()

                def port = findFreePort()
                def url = "jdbc:h2:tcp://localhost:$port/messageRepository;MODE=PostgreSQL"
                server = createTcpServer("-tcpPort $port -baseDir $workingDir".split(' ')).start()
                addShutdownHook {
                    stop()
                }

                dataSource = new JdbcDataSource(url: url,
                        user: 'sa', password: 'sa')
                setupSchema()
                LOG.info("Setup database in ${System.currentTimeMillis() - time} millis.")
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

    private stop() {
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
