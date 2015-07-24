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


// @formatter:off
/**
 * A {@link MessageBroker} implementation pre-configured to integrate with Spring's transactions and JDBC support.
 * It also expose configuration as setter, to simplify XML configuration.

<h2>Minimal Spring XML example</h2>
<blockquote><pre><code>
    &lt;bean id="minimallyConfiguredMessageBroker" class="org.openforis.rmb.spring.SpringJdbcMessageBroker"&gt;
        &lt;constructor-arg ref="dataSource"/&gt;
        &lt;property name="tablePrefix" value="example_"/&gt;
    &lt;/bean&gt;

    &lt;bean id="minimallyConfiguredQueue" class="org.openforis.rmb.spring.SpringMessageQueue"&gt;
        &lt;constructor-arg ref="minimallyConfiguredMessageBroker"/&gt;
        &lt;constructor-arg value="A queue"/&gt;
        &lt;constructor-arg&gt;
            &lt;list&gt;
                &lt;bean class="org.openforis.rmb.spring.SpringMessageConsumer"&gt;
                    &lt;constructor-arg value="A minimally configured consumer"/&gt;
                    &lt;constructor-arg ref="someMessageHandler"/&gt;
                &lt;/bean&gt;
            &lt;/list&gt;
        &lt;/constructor-arg&gt;
    &lt;/bean&gt;
</code></pre></blockquote>

<h2>Fully configured Spring XML example</h2>
<blockquote><pre><code>
    &lt;bean id="fullyConfiguredMessageBroker" class="org.openforis.rmb.spring.SpringJdbcMessageBroker"&gt;
        &lt;constructor-arg ref="dataSource"/&gt;
        &lt;property name="tablePrefix" value="example_"/&gt;
        &lt;property name="messageSerializer"&gt;
            &lt;bean class="org.openforis.rmb.objectserialization.ObjectSerializationMessageSerializer"/&gt;
        &lt;/property&gt;
        &lt;property name="monitors"&gt;
            &lt;list&gt;
                &lt;ref bean="eventCollectingMonitor"/&gt;
            &lt;/list&gt;
        &lt;/property&gt;
        &lt;property name="repositoryWatcherPollingPeriodSeconds" value="10"/&gt;
    &lt;/bean&gt;

    &lt;bean id="fullyConfiguredQueue" class="org.openforis.rmb.spring.SpringMessageQueue"&gt;
        &lt;constructor-arg ref="fullyConfiguredMessageBroker"/&gt;
        &lt;constructor-arg value="A queue"/&gt;
        &lt;constructor-arg&gt;
            &lt;list&gt;
                &lt;bean class="org.openforis.rmb.spring.SpringMessageConsumer"&gt;
                    &lt;constructor-arg value="A fully configured consumer"/&gt;
                    &lt;constructor-arg ref="someMessageHandler"/&gt;
                    &lt;property name="messagesHandledInParallel" value="1"/&gt;
                    &lt;property name="retries" value="5"/&gt;
                    &lt;property name="throttlingStrategy"&gt;
                        &lt;bean class="org.openforis.rmb.spi.ThrottlingStrategy$ExponentialBackoff"&gt;
                            &lt;constructor-arg value="1"/&gt;
                            &lt;constructor-arg value="MINUTES"/&gt;
                        &lt;/bean&gt;
                    &lt;/property&gt;
                    &lt;property name="timeoutSeconds" value="30"/&gt;
                &lt;/bean&gt;
            &lt;/list&gt;
        &lt;/constructor-arg&gt;
    &lt;/bean&gt;
</code></pre></blockquote>
 */
// @formatter:on
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
