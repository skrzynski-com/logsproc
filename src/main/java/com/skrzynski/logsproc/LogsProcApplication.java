package com.skrzynski.logsproc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.hsqldb.server.Server;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import java.nio.file.Paths;

@SpringBootApplication
public class LogsProcApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogsProcApplication.class, args);
    }

    @Bean
    public ObjectMapper createJacksonMapper() {
        return new ObjectMapper();
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public Server startHsqldbServer() {
        Server server = new Server();
        server.setDatabaseName(0, "logsdb");
        server.setDatabasePath(0, ".");
        return server;
    }

    @Bean
    @Profile("!test")
    CommandLineRunner loadData(LogsService logsService) {
        return args -> logsService.loadData(Paths.get(args[0]));
    }

}
