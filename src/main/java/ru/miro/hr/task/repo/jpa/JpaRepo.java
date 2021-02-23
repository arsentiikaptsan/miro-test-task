package ru.miro.hr.task.repo.jpa;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface JpaRepo extends JpaRepository<WidgetJpaImpl, Integer> {

    @Query(nativeQuery = true, value = "select * from widget where z >= ?1 order by z asc limit ?2")
    List<WidgetJpaImpl> findPageOrderedByZ(int from, int size);

    @Modifying
    @Query("delete from WidgetJpaImpl w where w.id = ?1")
    int deleteInOneQuery(int id);

    boolean existsByZ(int z);

    @Modifying
    @Query("update WidgetJpaImpl w set w.z = w.z + 1 where w.z >= ?1")
    int moveUp(int z);

    @Query(nativeQuery = true, value = "select max(z) from widget")
    Optional<Integer> findMaxZ();
}
