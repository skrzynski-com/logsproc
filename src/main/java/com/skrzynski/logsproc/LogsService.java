package com.skrzynski.logsproc;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.Files.lines;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Service
public class LogsService {

    private final LogsMapper logsMapper;
    private final ObjectMapper objectMapper;
    private final Semaphore throttle;
    private final AtomicInteger counter = new AtomicInteger();
    private static final Logger LOGGER = LoggerFactory.getLogger(LogsService.class);

    public LogsService(LogsMapper logsMapper, ObjectMapper objectMapper, @Value("${throttleLimit}") int throttleLimit) {
        this.logsMapper = logsMapper;
        this.objectMapper = objectMapper;
        this.throttle = new Semaphore(throttleLimit);
    }

    public void loadData(Path path) {
        LOGGER.debug("loadData() started with path: {}", path);
        try {
            counter.set(0); // this is command line program, that is why there is only one counter
            final CompletableFuture<Void> completionDetector = completedFuture(null);
            lines(path) // NOSONAR, stream is closed by reduction
                    .peek(this::throttleStream)
                    .reduce(completionDetector, (f, line) -> processLine(line, f), (f1, f2) -> f2)
                    .get(); // wait for the end of processing
            LOGGER.info("loadData() has loaded: {} lines", counter.get());
        } catch (Exception e) {
            LOGGER.error("loadData() encountered problem: {}", e.getMessage());
            throw new LogsServiceException(e);
        }
    }


    private void throttleStream(String line) { // NOSONAR siganture required by peek function
        try {
            throttle.acquire();
        } catch (InterruptedException e) { // NOSONAR, this is not service but command line application, so there will no interruption
            LOGGER.error("throttleStream() encountered problem: {}", e.getMessage());
            throw new LogsServiceException(e);
        }
    }

    private CompletableFuture<Void> processLine(String line, CompletableFuture<Void> completionDetector) {
        LOGGER.debug("processLine() started with line: {}", line);
        return completionDetector.thenCompose(aVoid -> processLine(line));
    }

    private CompletableFuture<Void> processLine(String line) {
        return supplyAsync(() -> deserializeLogEntry(line))
                .thenAcceptAsync(this::persistLogEntry)
                .thenRunAsync(this::releaseThrottle);
    }

    private LogEntry deserializeLogEntry(String line) {
        try {
            return objectMapper.readValue(line, LogEntry.class);
        } catch (IOException e) {
            LOGGER.error("deserializeLogEntry() encountered problem: {} with line: {}", e.getMessage(), line);
            throw new LogsServiceException(e);
        }
    }

    private void persistLogEntry(LogEntry logEntry) {
        try {
            LOGGER.debug("persistLogEntry() will insert: {}", logEntry);
            logsMapper.insert(logEntry);
        } catch (DuplicateKeyException e) {
            // first event has been already inserted
            LOGGER.debug("persistLogEntry() will update duration and alert flag: {}", logEntry);
            logsMapper.updateDuration(logEntry);
            logsMapper.updatAlert(logEntry);
        }
    }

    private void releaseThrottle() {
        throttle.release();
        counter.incrementAndGet();
    }

    public static class LogsServiceException extends RuntimeException {
        public LogsServiceException(Throwable cause) {
            super(cause);
        }
    }
}
