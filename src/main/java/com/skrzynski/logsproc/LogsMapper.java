package com.skrzynski.logsproc;

import org.apache.ibatis.annotations.*;

@Mapper
public interface LogsMapper {

    @Select("SELECT id, state, timestamp, type, host, duration, alert, version " +
            "FROM logs " +
            "WHERE id = #{id}")
    LogEntry findById(String id);

    @Insert("INSERT INTO logs (id, state, timestamp, type, host, duration, alert, version) " +
            "VALUES(#{id}, #{state}, #{timestamp}, #{type}, #{host}, #{duration}, #{alert}, #{version})")
    void insert(LogEntry logEntry);

    @Update("UPDATE logs SET duration = ABS(timestamp-#{timestamp}), version=1 WHERE id = #{id} AND version=0")
    long updateDuration(LogEntry logEntry);

    @Update("UPDATE logs SET alert=true WHERE id = #{id} AND version=1 AND duration>4")
    long updatAlert(LogEntry logEntry);


    @Delete("TRUNCATE TABLE logs")
    void truncate();

}
