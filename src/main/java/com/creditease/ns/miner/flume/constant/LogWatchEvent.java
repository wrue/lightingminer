package com.creditease.ns.miner.flume.constant;

import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchService;



/**
 * Defines the <em>custom</em> event kinds.
 *
 * @since 1.7
 */

public final class LogWatchEvent {
    private LogWatchEvent() { }

    /**
     * A special event to indicate that events may have been lost or
     * discarded.
     *
     * <p> The {@link WatchEvent#context context} for this event is
     * implementation specific and may be {@code null}. The event {@link
     * WatchEvent#count count} may be greater than {@code 1}.
     *
     * @see WatchService
     */
    public static final WatchEvent.Kind<Path> AGENTRESTARTEVENT =
        new LogStdWatchEventKind<Path>("AGENTRESTARTEVENT", Path.class);



    private static class LogStdWatchEventKind<T> implements WatchEvent.Kind<T> {
        private final String name;
        private final Class<T> type;
        LogStdWatchEventKind(String name, Class<T> type) {
            this.name = name;
            this.type = type;
        }
        @Override public String name() { return name; }
        @Override public Class<T> type() { return type; }
        @Override public String toString() { return name; }
    }
}

