/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.node.tasks.list;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.list.proto.ListTasksProto;
import org.opensearch.action.support.tasks.BaseTasksRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.ProtobufPayload;

import java.io.IOException;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * A request to get node tasks
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ListTasksRequest extends BaseTasksRequest<ListTasksRequest> implements ProtobufPayload {

    /** First version that encodes this request as protobuf on the wire. */
    public static final Version PROTO_SINCE = Version.V_3_10_0;

    private ListTasksProto.ListTasksRequest message;

    public ListTasksRequest() {
        this(ListTasksProto.ListTasksRequest.getDefaultInstance());
    }

    /** Wrap an existing message. Package-private: callers outside this package use the public constructor and setters. */
    ListTasksRequest(ListTasksProto.ListTasksRequest message) {
        this.message = Objects.requireNonNull(message, "message");
    }

    public ListTasksRequest(StreamInput in) throws IOException {
        super(in); // header fields
        this.message = ProtobufPayload.readPayload(
            in,
            PROTO_SINCE,
            ListTasksProto.ListTasksRequest::parseFrom,
            ListTasksRequest::readLegacy
        );
    }

    // header fields and payload are written by TransportRequest#writeTo; kept here for binary compatibility
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public Version protobufSince() {
        return PROTO_SINCE;
    }

    @Override
    public byte[] protobufBytes() {
        return message.toByteArray();
    }

    /** The current message. Immutable; setters replace it. Package-private: code outside this package reads the accessors. */
    ListTasksProto.ListTasksRequest message() {
        return message;
    }

    private ListTasksRequest update(UnaryOperator<ListTasksProto.ListTasksRequest> change) {
        this.message = Objects.requireNonNull(change.apply(message), "message");
        return this;
    }

    // ----- frozen legacy layout -----

    private static ListTasksProto.ListTasksRequest readLegacy(StreamInput in) throws IOException {
        return ListTasksProto.ListTasksRequest.newBuilder().setDetailed(in.readBoolean()).setWaitForCompletion(in.readBoolean()).build();
    }

    @Override
    protected void writeLegacy(StreamOutput out) throws IOException {
        out.writeBoolean(getDetailed());
        out.writeBoolean(getWaitForCompletion());
    }

    // ----- public API (unchanged) -----

    /**
     * Should the detailed task information be returned.
     */
    public boolean getDetailed() {
        return message.getDetailed();
    }

    /**
     * Should the detailed task information be returned.
     */
    public ListTasksRequest setDetailed(boolean detailed) {
        return update(m -> m.toBuilder().setDetailed(detailed).build());
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public boolean getWaitForCompletion() {
        return message.getWaitForCompletion();
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public ListTasksRequest setWaitForCompletion(boolean waitForCompletion) {
        return update(m -> m.toBuilder().setWaitForCompletion(waitForCompletion).build());
    }

}
