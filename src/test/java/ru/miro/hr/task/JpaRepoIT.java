package ru.miro.hr.task;

import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.persistence.EntityManager;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.profiles.active=jpa_repo", "transaction.timeout=100"})
class JpaRepoIT extends IT {

    @Autowired
    EntityManager em;

    @Autowired
    PlatformTransactionManager transactionManager;

    @AfterEach
    public void cleanUp() {
        new TransactionTemplate(transactionManager)
                .executeWithoutResult(status -> em.createNativeQuery("truncate table widget").executeUpdate());
    }
}
