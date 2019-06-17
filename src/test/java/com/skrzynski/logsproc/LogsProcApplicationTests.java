package com.skrzynski.logsproc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class LogsProcApplicationTests {

    @Autowired
    LogsMapper mapper;

    @Autowired
    LogsService logsService;

    @Before
    public void truncate() {
        mapper.truncate();
    }

    @Test
    public void loadDataShouldCorrectlyMarkAlertField() {
        // when
        logsService.loadData(Paths.get("src", "test", "resources", "logs.txt"));

        // then
        LogEntry logEntry1 = mapper.findById("scsmbstgra");
        assertThat(logEntry1).extracting(LogEntry::getType).isEqualTo("APPLICATION_LOG");
        assertThat(logEntry1).extracting(LogEntry::getHost).isEqualTo("12345");
        assertThat(logEntry1).extracting(LogEntry::getDuration).isEqualTo(5L);
        assertThat(logEntry1).extracting(LogEntry::isAlert).isEqualTo(true);

        LogEntry logEntry2 = mapper.findById("scsmbstgrb");
        assertThat(logEntry2).extracting(LogEntry::getDuration).isEqualTo(3L);
        assertThat(logEntry2).extracting(LogEntry::isAlert).isEqualTo(false);

        LogEntry logEntry3 = mapper.findById("scsmbstgrc");
        assertThat(logEntry3).extracting(LogEntry::getDuration).isEqualTo(8L);
        assertThat(logEntry3).extracting(LogEntry::isAlert).isEqualTo(true);
    }

}
