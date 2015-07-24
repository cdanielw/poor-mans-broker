package org.openforis.rmb.spring;

import org.openforis.rmb.MessageBroker;
import org.openforis.rmb.MessageQueue;
import org.openforis.rmb.RepositoryMessageBroker;
import org.openforis.rmb.jdbc.JdbcMessageRepository;
import org.openforis.rmb.monitor.Event;
import org.openforis.rmb.monitor.Monitor;
import org.openforis.rmb.spi.MessageSerializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class SpringJdbcMessageBroker implements MessageBroker, InitializingBean, SmartLifecycle {
    private final AtomicBoolean running = new AtomicBoolean();
    private final DataSource dataSource;
    private MessageBroker messageBroker;

    private String tablePrefix = "";
    private MessageSerializer messageSerializer;
    private List<Monitor<Event>> monitors;
    private Long repositoryWatcherPollingPeriodSeconds;

    public SpringJdbcMessageBroker(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void afterPropertiesSet() throws Exception {
        RepositoryMessageBroker.Builder builder = RepositoryMessageBroker.builder(
                new JdbcMessageRepository(
                        new SpringJdbcConnectionManager(dataSource),
                        tablePrefix
                ),
                new SpringTransactionSynchronizer(dataSource)
        );

        if (messageSerializer != null)
            builder.messageSerializer(messageSerializer);
        if (monitors != null)
            for (Monitor<Event> monitor : monitors)
                builder.monitor(monitor);
        if (repositoryWatcherPollingPeriodSeconds != null)
            builder.repositoryWatcherPollingSchedule(repositoryWatcherPollingPeriodSeconds, SECONDS);

        messageBroker = builder.build();
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setMessageSerializer(MessageSerializer messageSerializer) {
        this.messageSerializer = messageSerializer;
    }

    public void setMonitors(List<Monitor<Event>> monitors) {
        this.monitors = monitors;
    }

    public void setRepositoryWatcherPollingPeriodSeconds(Long repositoryWatcherPollingPeriodSeconds) {
        this.repositoryWatcherPollingPeriodSeconds = repositoryWatcherPollingPeriodSeconds;
    }

    public void start() {
        running.set(true);
        messageBroker.start();
    }

    public void stop() {
        running.set(false);
        messageBroker.stop();
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isAutoStartup() {
        return true;
    }

    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    public int getPhase() {
        return 0;
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        return messageBroker.queueBuilder(queueId, messageType);
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId) {
        return messageBroker.queueBuilder(queueId);
    }
}
