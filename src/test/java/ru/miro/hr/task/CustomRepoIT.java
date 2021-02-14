package ru.miro.hr.task;

import org.junit.jupiter.api.AfterEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.miro.hr.task.repo.custom.CustomRepo;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "spring.profiles.active=custom_repo")
class CustomRepoIT extends IT {

    @Autowired
    CustomRepo customRepo;

    @AfterEach
    public void cleanUp() {
        customRepo.clear();
    }

}
