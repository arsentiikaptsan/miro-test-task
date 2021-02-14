package ru.miro.hr.task.repo.custom;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.miro.hr.task.model.Widget;

@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
class WidgetCustomImpl implements Widget {

    // Remember, we employ copy-on-write strategy so these fields are readonly (and don't have to be volatile)!
    @EqualsAndHashCode.Include
    private final int id, x, y, z, width, height;

    // These are ids, that write transactions get in the begging of execution.
    // To reduce contention, "real" serial numbers are assigned at the very end of execution
    // and are stored in the transaction log (it happens as one atomic operation).
    // By referring to the log, readers can deduct if transaction that produced the record (stored in fromTransactionId),
    // is committed an is not "in the future". The same goes for tillTransactionId.
    private final int fromTransactionId;
    private volatile int tillTransactionId = -1;

    // Cache for faster access during scanning (by bypassing the transaction log).
    // P.S. Serials start at 0, so we can see if cache was set or not and we have to refer to the log.
    private int fromTransactionSerial = -1, tillTransactionSerial = -1;
}
