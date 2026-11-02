/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.protoaction;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.proto.ActiveShardCountProto;
import org.opensearch.action.support.proto.IndicesOptionsProto;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.proto.ClusterHealthStatusProto;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.proto.PriorityProto;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.unit.proto.TimeValueProto;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Conversions between shared leaf domain types and their generated protobuf forms. These exist for the
 * compatibility window in which legacy {@code Writeable} containers embed the Java types; messages that are
 * composed entirely of generated types need none of this.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ProtoConverters {

    private ProtoConverters() {}

    // ----- TimeValue -----

    public static TimeValueProto.TimeValue toProto(TimeValue value) {
        return TimeValueProto.TimeValue.newBuilder().setDuration(value.duration()).setTimeUnit(toProto(value.timeUnit())).build();
    }

    public static TimeValue fromProto(TimeValueProto.TimeValue value) {
        return new TimeValue(value.getDuration(), fromProto(value.getTimeUnit()));
    }

    static TimeValueProto.TimeValue.TimeUnit toProto(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_NANOSECONDS;
            case MICROSECONDS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_MICROSECONDS;
            case MILLISECONDS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_MILLISECONDS;
            case SECONDS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_SECONDS;
            case MINUTES:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_MINUTES;
            case HOURS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_HOURS;
            case DAYS:
                return TimeValueProto.TimeValue.TimeUnit.TIME_UNIT_DAYS;
            default:
                throw new IllegalArgumentException("unknown time unit " + unit);
        }
    }

    static TimeUnit fromProto(TimeValueProto.TimeValue.TimeUnit unit) {
        switch (unit) {
            case TIME_UNIT_NANOSECONDS:
                return TimeUnit.NANOSECONDS;
            case TIME_UNIT_MICROSECONDS:
                return TimeUnit.MICROSECONDS;
            case TIME_UNIT_MILLISECONDS:
                return TimeUnit.MILLISECONDS;
            case TIME_UNIT_SECONDS:
                return TimeUnit.SECONDS;
            case TIME_UNIT_MINUTES:
                return TimeUnit.MINUTES;
            case TIME_UNIT_HOURS:
                return TimeUnit.HOURS;
            case TIME_UNIT_DAYS:
                return TimeUnit.DAYS;
            default:
                throw new IllegalArgumentException("unknown or unspecified time unit " + unit);
        }
    }

    // ----- Priority -----

    public static PriorityProto.Priority toProto(Priority priority) {
        switch (priority) {
            case IMMEDIATE:
                return PriorityProto.Priority.PRIORITY_IMMEDIATE;
            case URGENT:
                return PriorityProto.Priority.PRIORITY_URGENT;
            case HIGH:
                return PriorityProto.Priority.PRIORITY_HIGH;
            case NORMAL:
                return PriorityProto.Priority.PRIORITY_NORMAL;
            case LOW:
                return PriorityProto.Priority.PRIORITY_LOW;
            case LANGUID:
                return PriorityProto.Priority.PRIORITY_LANGUID;
            default:
                throw new IllegalArgumentException("unknown priority " + priority);
        }
    }

    public static Priority fromProto(PriorityProto.Priority priority) {
        switch (priority) {
            case PRIORITY_IMMEDIATE:
                return Priority.IMMEDIATE;
            case PRIORITY_URGENT:
                return Priority.URGENT;
            case PRIORITY_HIGH:
                return Priority.HIGH;
            case PRIORITY_NORMAL:
                return Priority.NORMAL;
            case PRIORITY_LOW:
                return Priority.LOW;
            case PRIORITY_LANGUID:
                return Priority.LANGUID;
            default:
                throw new IllegalArgumentException("unknown or unspecified priority " + priority);
        }
    }

    // ----- ClusterHealthStatus -----

    public static ClusterHealthStatusProto.ClusterHealthStatus toProto(ClusterHealthStatus status) {
        switch (status) {
            case GREEN:
                return ClusterHealthStatusProto.ClusterHealthStatus.CLUSTER_HEALTH_STATUS_GREEN;
            case YELLOW:
                return ClusterHealthStatusProto.ClusterHealthStatus.CLUSTER_HEALTH_STATUS_YELLOW;
            case RED:
                return ClusterHealthStatusProto.ClusterHealthStatus.CLUSTER_HEALTH_STATUS_RED;
            default:
                throw new IllegalArgumentException("unknown cluster health status " + status);
        }
    }

    public static ClusterHealthStatus fromProto(ClusterHealthStatusProto.ClusterHealthStatus status) {
        switch (status) {
            case CLUSTER_HEALTH_STATUS_GREEN:
                return ClusterHealthStatus.GREEN;
            case CLUSTER_HEALTH_STATUS_YELLOW:
                return ClusterHealthStatus.YELLOW;
            case CLUSTER_HEALTH_STATUS_RED:
                return ClusterHealthStatus.RED;
            default:
                throw new IllegalArgumentException("unknown or unspecified cluster health status " + status);
        }
    }

    // ----- ActiveShardCount -----

    public static ActiveShardCountProto.ActiveShardCount toProto(ActiveShardCount count) {
        final ActiveShardCountProto.ActiveShardCount.Builder builder = ActiveShardCountProto.ActiveShardCount.newBuilder();
        if (count.equals(ActiveShardCount.ALL)) {
            builder.setSentinel(ActiveShardCountProto.ActiveShardCount.Sentinel.SENTINEL_ALL);
        } else if (count.equals(ActiveShardCount.DEFAULT)) {
            builder.setSentinel(ActiveShardCountProto.ActiveShardCount.Sentinel.SENTINEL_DEFAULT);
        } else {
            // ActiveShardCount only exposes its value through toString for non-sentinel counts
            builder.setCount(Integer.parseInt(count.toString()));
        }
        return builder.build();
    }

    public static ActiveShardCount fromProto(ActiveShardCountProto.ActiveShardCount count) {
        switch (count.getKindCase()) {
            case COUNT:
                return ActiveShardCount.from(count.getCount());
            case SENTINEL:
                switch (count.getSentinel()) {
                    case SENTINEL_ALL:
                        return ActiveShardCount.ALL;
                    case SENTINEL_DEFAULT:
                        return ActiveShardCount.DEFAULT;
                    default:
                        throw new IllegalArgumentException("unknown or unspecified active shard count sentinel " + count.getSentinel());
                }
            default:
                // unset: treat as the ActiveShardCount default of "not specified"
                return ActiveShardCount.DEFAULT;
        }
    }

    // ----- IndicesOptions -----

    public static IndicesOptionsProto.IndicesOptions toProto(IndicesOptions options) {
        return IndicesOptionsProto.IndicesOptions.newBuilder()
            .setIgnoreUnavailable(options.ignoreUnavailable())
            .setIgnoreAliases(options.ignoreAliases())
            .setAllowNoIndices(options.allowNoIndices())
            .setForbidAliasesToMultipleIndices(options.allowAliasesToMultipleIndices() == false)
            .setForbidClosedIndices(options.forbidClosedIndices())
            .setIgnoreThrottled(options.ignoreThrottled())
            .setExpandWildcardsOpen(options.expandWildcardsOpen())
            .setExpandWildcardsClosed(options.expandWildcardsClosed())
            .setExpandWildcardsHidden(options.expandWildcardsHidden())
            .build();
    }

    public static IndicesOptions fromProto(IndicesOptionsProto.IndicesOptions options) {
        final EnumSet<IndicesOptions.Option> opts = EnumSet.noneOf(IndicesOptions.Option.class);
        final EnumSet<IndicesOptions.WildcardStates> wildcards = EnumSet.noneOf(IndicesOptions.WildcardStates.class);
        if (options.getIgnoreUnavailable()) {
            opts.add(IndicesOptions.Option.IGNORE_UNAVAILABLE);
        }
        if (options.getIgnoreAliases()) {
            opts.add(IndicesOptions.Option.IGNORE_ALIASES);
        }
        if (options.getAllowNoIndices()) {
            opts.add(IndicesOptions.Option.ALLOW_NO_INDICES);
        }
        if (options.getForbidAliasesToMultipleIndices()) {
            opts.add(IndicesOptions.Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (options.getForbidClosedIndices()) {
            opts.add(IndicesOptions.Option.FORBID_CLOSED_INDICES);
        }
        if (options.getIgnoreThrottled()) {
            opts.add(IndicesOptions.Option.IGNORE_THROTTLED);
        }
        if (options.getExpandWildcardsOpen()) {
            wildcards.add(IndicesOptions.WildcardStates.OPEN);
        }
        if (options.getExpandWildcardsClosed()) {
            wildcards.add(IndicesOptions.WildcardStates.CLOSED);
        }
        if (options.getExpandWildcardsHidden()) {
            wildcards.add(IndicesOptions.WildcardStates.HIDDEN);
        }
        return new IndicesOptions(opts, wildcards);
    }
}
