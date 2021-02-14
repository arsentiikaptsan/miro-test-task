package ru.miro.hr.task.repo.custom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TransactionLogUT {

    private final TransactionLog transactionLog = new TransactionLog(2);

    @AfterEach
    public void reset() {
        transactionLog.clear();
    }

    @Test
    public void testCommitAndGetTransactionSerial() {
        int serial1 = transactionLog.commit(1);
        int serial2 = transactionLog.commit(2);

        assertThat(transactionLog.getTransactionSerial(1)).isEqualTo(serial1);
        assertThat(transactionLog.getTransactionSerial(2)).isEqualTo(serial2);
        assertThat(serial2).isGreaterThan(serial1);
    }

    @Test
    public void testUncommittedTransactionIdRerunsNullSerial() {
        assertThat(transactionLog.getTransactionSerial(1)).isNull();
    }

    @Test
    public void testLatestSerial() {
        int serial = transactionLog.commit(1);

        assertThat(transactionLog.getLatestSerial()).isEqualTo(serial);
    }

    @Test
    public void testCommittingSameTransactionTwice() {
        transactionLog.commit(1);

        assertThrows(IllegalStateException.class, () -> transactionLog.commit(1));
    }

    @Test
    public void testClear() {
        transactionLog.commit(1);
        transactionLog.clear();

        assertThat(transactionLog.getLatestSerial()).isEqualTo(-1);
    }
}
