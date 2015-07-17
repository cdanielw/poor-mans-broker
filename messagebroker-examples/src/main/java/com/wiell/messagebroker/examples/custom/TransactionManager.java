package com.wiell.messagebroker.examples.custom;

import java.util.concurrent.Callable;

public interface TransactionManager {
    <T> T withTransaction(Callable<T> unitOfWork);
}
