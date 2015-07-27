package org.openforis.rmb.spi;

/**
 * Integrates a message queue with the application transaction manager.
 */
public interface TransactionSynchronizer {
    /**
     * A fake implementation which always is considered to be in a transaction and directly notifies a commit listener.
     */
    TransactionSynchronizer NULL_TRANSACTION_SYNCHRONIZER = new TransactionSynchronizer() {
        public boolean isInTransaction() {
            return true;
        }

        public void notifyOnCommit(CommitListener listener) {
            listener.committed();
        }
    };

    /**
     * Determines if current thread is in a transaction.
     *
     * @return true if in a transaction
     */
    boolean isInTransaction();

    /**
     * Invokes callback when transaction commits.
     *
     * @param listener the listener to invoke on commit
     */
    void notifyOnCommit(CommitListener listener);

    /**
     * Callback for commits.
     */
    interface CommitListener {
        /**
         * Invoked when a transaction commits.
         */
        void committed();
    }
}
