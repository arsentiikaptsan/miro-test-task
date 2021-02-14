package ru.miro.hr.task.repo.custom;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class UniqueKeyUT {

    private final Random random = new Random();
    private final UniqueKeyFactory uniqueKeyFactory = new UniqueKeyFactory();

    @Test
    public void testUniqueness() {
        int z = random.nextInt();

        assertThat(uniqueKeyFactory.makeUnique(z))
                .isNotEqualTo(uniqueKeyFactory.makeUnique(z));
    }

    @Test
    public void testOrder() {
        var uk1 = new UniqueKey(1, 1);
        var uk2 = new UniqueKey(1, 2);
        var uk3 = new UniqueKey(2, 1);

        assertThat(uk1).isLessThan(uk2);
        assertThat(uk2).isLessThan(uk3);
    }

    @Test
    public void testUniqueKeyFabricReset() {
        int z = random.nextInt();
        var o1 = uniqueKeyFactory.makeUnique(z);

        uniqueKeyFactory.reset();

        var o2 = uniqueKeyFactory.makeUnique(z);

        assertThat(o1).isEqualTo(o2);
    }
}
