package org.openforis.rmb.spring;

import org.openforis.rmb.spi.TransactionSynchronizer;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;

public final class SpringTransactionSynchronizer implements TransactionSynchronizer {
    private final DataSource dataSource;

    public SpringTransactionSynchronizer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isInTransaction() {
        DataSource targetDataSource = determineTargetDataSource();
        ConnectionHolder conHolder = (ConnectionHolder) TransactionSynchronizationManager.getResource(targetDataSource);
        return conHolder != null;
    }

    public void notifyOnCommit(final CommitListener listener) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {
            public void afterCommit() {
                listener.committed();
            }
        });
    }

    private DataSource determineTargetDataSource() {
        if (dataSource instanceof DelegatingDataSource) {
            return ((DelegatingDataSource) dataSource).getTargetDataSource();
        } else {
            return dataSource;
        }
    }
}
