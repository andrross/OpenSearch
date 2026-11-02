/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Base classes for request and response types whose state is a generated protobuf message. The existing request and
 * response classes become thin envelopes over a message, choosing the protobuf or the frozen legacy encoding per
 * peer version. Their accessors apply defaults and convert to domain types; the generated types stay inside the
 * class's package. Transport actions are unchanged: they read requests and build responses through the same public
 * accessors and constructors as before. Action names, public request/response APIs and the transport protocol are
 * unchanged.
 */
package org.opensearch.action.support.protoaction;
