package com.wiell.messagebroker.spi;

public interface TransactionSynchronizer {
    TransactionSynchronizer NULL_TRANSACTION_SYNCHRONIZER = new TransactionSynchronizer() {
        public boolean isInTransaction() {
            return true;
        }

        public void notifyOnCommit(CommitListener listener) {
            listener.committed();
        }
    };

    boolean isInTransaction();

    void notifyOnCommit(CommitListener listener);

    interface CommitListener {
        void committed();
    }
}
