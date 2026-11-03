/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.transport;

import org.opensearch.Version;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Marks a transport message whose action-specific payload is a protobuf message.
 * <p>
 * Every transport message is written as <em>header fields</em> (task, routing and timeout metadata owned by the
 * base classes) followed by a <em>payload</em> (the action-specific fields). For a message implementing this
 * interface the payload is the serialized protobuf message when the peer is at or after {@link #protobufSince()},
 * and the frozen pre-protobuf layout otherwise. The root classes ({@code TransportRequest},
 * {@link TransportResponse}) apply that rule; implementors only supply the bytes and the version.
 * <p>
 * This interface lives in {@code libs/core}, which has no protobuf dependency, so it deals in {@code byte[]}. The
 * typed message is held by the implementing class.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ProtobufPayload {

    /** First version that understands the protobuf encoding of this message's payload. */
    Version protobufSince();

    /** The serialized protobuf message. */
    byte[] protobufBytes();

    /**
     * The serialized protobuf message for a specific peer.
     * <p>
     * Most payloads are the same bytes for every peer and need only {@link #protobufBytes()}. A payload that embeds an
     * extension point whose encoding is chosen by the extension (a {@code NamedWriteable} frame carrying another
     * class's bytes) depends on the peer, because the embedded class applies its own version rules. Such a payload
     * overrides this method and builds the message for {@code peerVersion}.
     */
    default byte[] protobufBytes(Version peerVersion) {
        return protobufBytes();
    }

    /**
     * Whether {@code version} (a peer's version) receives the protobuf payload rather than the legacy layout.
     */
    static boolean useProtobuf(ProtobufPayload payload, Version version) {
        return version.onOrAfter(payload.protobufSince());
    }

    /**
     * Write the payload: protobuf bytes for a new enough peer, otherwise {@code legacy}.
     */
    static void writePayload(ProtobufPayload payload, StreamOutput out, LegacyWriter legacy) throws IOException {
        if (useProtobuf(payload, out.getVersion())) {
            out.writeByteArray(payload.protobufBytes(out.getVersion()));
        } else {
            legacy.write(out);
        }
    }

    /**
     * Read the payload written by {@link #writePayload}: parse protobuf bytes for a new enough peer, otherwise run
     * {@code legacy}. Called from the leaf constructor after the base-class constructors have consumed the header
     * fields.
     *
     * @param since the value the leaf will return from {@link #protobufSince()}
     * @param parser parses the serialized message
     * @param legacy reads the frozen pre-protobuf layout and produces the same message type
     */
    static <M> M readPayload(
        StreamInput in,
        Version since,
        CheckedFunction<byte[], M, IOException> parser,
        CheckedFunction<StreamInput, M, IOException> legacy
    ) throws IOException {
        if (in.getVersion().onOrAfter(since)) {
            return parser.apply(in.readByteArray());
        }
        return legacy.apply(in);
    }

    /**
     * Writes the frozen pre-protobuf layout of a payload.
     */
    @FunctionalInterface
    interface LegacyWriter {
        void write(StreamOutput out) throws IOException;
    }
}
