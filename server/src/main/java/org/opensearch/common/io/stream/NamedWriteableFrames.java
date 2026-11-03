/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import com.google.protobuf.ByteString;
import org.opensearch.Version;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.io.stream.proto.NamedWriteableProto;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Carries a {@link NamedWriteable} inside a protobuf message.
 * <p>
 * A {@code NamedWriteable} is an extension point: the concrete class is chosen at runtime from a
 * {@link NamedWriteableRegistry}, often by a plugin, so a container's schema cannot name it. The frame is protobuf
 * (the registry name and the class's bytes); the payload is whatever the class writes to a nested stream pinned to
 * the peer's version. An unmodified class therefore keeps working, with its own version rules intact, and a class
 * that implements {@code ProtobufPayload} writes its message by its own decision. The container never knows which.
 * <p>
 * Because the payload depends on the peer version, a container holding a framed extension cannot be a single frozen
 * message; it builds the frame when it writes (see {@code ProtobufPayload#protobufBytes(Version)}).
 *
 * @opensearch.internal
 */
@InternalApi
public final class NamedWriteableFrames {

    private NamedWriteableFrames() {}

    /**
     * Frame {@code writeable} for a peer at {@code peerVersion}.
     */
    public static NamedWriteableProto.NamedWriteable toProto(NamedWriteable writeable, Version peerVersion) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(peerVersion);
            writeable.writeTo(out);
            return NamedWriteableProto.NamedWriteable.newBuilder()
                .setName(writeable.getWriteableName())
                .setPayload(ByteString.copyFrom(BytesReference.toBytes(out.bytes())))
                .build();
        }
    }

    /**
     * Read the {@link NamedWriteable} in {@code frame}, using the registry and version of {@code context}, the
     * stream the containing message was read from.
     *
     * @throws UnsupportedOperationException if {@code context} has no registry, matching
     *         {@link StreamInput#readNamedWriteable(Class)}
     * @throws IllegalArgumentException if the frame's name is unknown to the registry, matching
     *         {@link NamedWriteableRegistry#getReader(Class, String)}
     * @throws IOException if the payload is malformed or not fully consumed
     */
    public static <C extends NamedWriteable> C fromProto(
        NamedWriteableProto.NamedWriteable frame,
        Class<C> categoryClass,
        StreamInput context
    ) throws IOException {
        final NamedWriteableRegistry registry = context.namedWriteableRegistry();
        if (registry == null) {
            throw new UnsupportedOperationException("can't read named writeable from a stream without a registry");
        }
        final Writeable.Reader<? extends C> reader = registry.getReader(categoryClass, frame.getName());
        try (StreamInput nested = new NamedWriteableAwareStreamInput(StreamInput.wrap(frame.getPayload().toByteArray()), registry)) {
            nested.setVersion(context.getVersion());
            final C value = reader.read(nested);
            if (value == null) {
                throw new IOException("Writeable.Reader [" + reader + "] returned null for [" + frame.getName() + "]");
            }
            if (nested.available() != 0) {
                throw new IOException(
                    "["
                        + frame.getName()
                        + "] of category ["
                        + categoryClass.getSimpleName()
                        + "] left "
                        + nested.available()
                        + " unread bytes in its payload"
                );
            }
            return value;
        }
    }
}
