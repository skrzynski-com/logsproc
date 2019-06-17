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
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.file.Files.lines;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

@Service
public class LogsService {

    private final LogsMapper logsMapper;
    private final ObjectMapper objectMapper;
    private final Semaphore throttle;
    private final AtomicInteger counter = new AtomicInteger();
    private final static Logger LOGGER = LoggerFactory.getLogger(LogsService.class);

    public LogsService(LogsMapper logsMapper, ObjectMapper objectMapper, @Value("${throttleLimit}") int throttleLimit) {
        this.logsMapper = logsMapper;
        this.objectMapper = objectMapper;
        this.throttle = new Semaphore(throttleLimit);
    }

    public void loadData(Path path) {
        LOGGER.debug("loadData() started with path: " + path);
        try {
            counter.set(0); // this is command line program, that is why there is only one counter
            final CompletableFuture<Void> completionDetector = completedFuture(null);
            lines(path) // NOSONAR, stream is closed by reduction
                    .reduce(completionDetector, (f, line) -> processLine(line, f), (f1, f2) -> f2)
                    .get(); // wait for the end of processing
            LOGGER.info("loadData() has loaded: " + counter.get() + "lines");
        } catch (Exception e) {
            LOGGER.error("loadData() encountered problem: " + e.getMessage(), e);
            throw new LogsServiceException(e);
        }
    }

    private CompletableFuture<Void> processLine(String line, CompletableFuture<Void> completionDetector) {
        LOGGER.debug("processLine() started with line: " + line);
        CompletableFuture<Void> result = completionDetector;
        try {
            throttle.acquire();
            CompletableFuture<Void> future = supplyAsync(() -> deserializeLogEntry(line))
                    .thenApplyAsync(persistLine())
                    .thenAcceptAsync(releaseThrottle());

            result = result.thenCompose(aVoid -> future);
        } catch (InterruptedException e) { // NOSONAR, this is not service but command line application, so there will no interruption
            LOGGER.error("processLine() encountered problem: " + e.getMessage(), e);
            throw new LogsServiceException(e);
        }

        return result;
    }

    private Function<LogEntry, LogEntry> persistLine() {
        return logEntry -> {
            try {
                LOGGER.debug("processLine() will insert: " + logEntry);
                logsMapper.insert(logEntry);
            } catch (DuplicateKeyException e) {
                // first event has been already inserted
                LOGGER.debug("processLine() will update duration and alert flag: " + logEntry);
                logsMapper.updateDuration(logEntry);
                logsMapper.updatAlert(logEntry);
            }
            return logEntry;
        };
    }

    private Consumer<LogEntry> releaseThrottle() {
        return le -> {
            throttle.release();
            counter.incrementAndGet();
        };
    }

    private LogEntry deserializeLogEntry(String line) {
        try {
            return objectMapper.readValue(line, LogEntry.class);
        } catch (IOException e) {
            LOGGER.error("deserializeLogEntry() encountered problem: " + e.getMessage() + " with line: " + line, e);
            throw new LogsServiceException(e);
        }
    }

    public static class LogsServiceException extends RuntimeException {
        public LogsServiceException(Throwable cause) {
            super(cause);
        }
    }
}
