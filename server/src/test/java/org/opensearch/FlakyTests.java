/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import java.util.concurrent.ThreadLocalRandom;

import org.opensearch.test.OpenSearchTestCase;

public class FlakyTests extends OpenSearchTestCase {
    public void testThatIsFlaky() {
        // This should fail half the time, regardless of random seed
        assertTrue(ThreadLocalRandom.current().nextBoolean());
    }

    public void testThatIsFlaky2() {
        // This should fail half the time, regardless of random seed
        assertTrue(ThreadLocalRandom.current().nextBoolean());
    }
}
