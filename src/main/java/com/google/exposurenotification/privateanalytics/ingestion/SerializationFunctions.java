/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.exposurenotification.privateanalytics.ingestion;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.abetterinternet.prio.v1.PrioDataSharePacket;
import org.abetterinternet.prio.v1.PrioIngestionHeader;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder of various DoFn's for aspects of serialization
 */
public class SerializationFunctions {

    public static class SerializeDataShareFn extends DoFn<DataShare, List<PrioDataSharePacket>> {
        private static final Logger LOG = LoggerFactory.getLogger(SerializeDataShareFn.class);
        private final int numberOfServers;
        private final Counter dataShareIncluded = Metrics
                .counter(SerializeDataShareFn.class, "dataShareIncluded");
        private final Counter dataSharesWrongNumberServers = Metrics
                .counter(SerializeDataShareFn.class, "dataSharesWrongNumberServers");

        public SerializeDataShareFn(int numberOfServers) {
            this.numberOfServers = numberOfServers;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            List<Map<String, String>> encryptedDataShares = c.element().getEncryptedDataShares();
            if (encryptedDataShares.size() != numberOfServers) {
                dataSharesWrongNumberServers.inc();
                LOG.trace("Excluded element: " + c.element());
                return;
            }

            List<PrioDataSharePacket> splitDataShares = new ArrayList<>();
            for (Map<String, String> dataShare : encryptedDataShares) {
                splitDataShares.add(
                        PrioDataSharePacket.newBuilder()
                                .setEncryptionKeyId(dataShare.get(DataShare.ENCRYPTION_KEY_ID))
                                .setEncryptedPayload(
                                        ByteBuffer.wrap(
                                                dataShare.get(DataShare.DATA_SHARE_PAYLOAD).getBytes()))
                                .setRPit(c.element().getRPit())
                                .setUuid(c.element().getUuid())
                                .build()
                );
            }
            dataShareIncluded.inc();
            c.output(splitDataShares);
        }
    }

    public static class SerializeIngestionHeaderFn extends DoFn<DataShare, PrioIngestionHeader> {
        private final ValueProvider<Long> startTime;
        private final ValueProvider<Long> duration;
        private final ValueProvider<String> batchUuid;

        public SerializeIngestionHeaderFn(ValueProvider<Long> startTime, ValueProvider<Long> duration) {
            this.startTime = startTime;
            this.duration = duration;
            // TODO: generate batch uuid
            this.batchUuid = StaticValueProvider.of("placeholderUuid");
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(
                    PrioIngestionHeader.newBuilder()
                            .setBatchUuid(batchUuid.get())
                            .setName("BatchUuid=" + batchUuid.get())
                            .setBatchStartTime(startTime.get())
                            .setBatchEndTime(startTime.get() + duration.get())
                            .setNumberOfServers(c.element().getNumberOfServers())
                            .setBins(c.element().getBins())
                            .setHammingWeight(c.element().getHammingWeight())
                            .setPrime(c.element().getPrime())
                            .setEpsilon(c.element().getEpsilon())
                            // TODO: implement packet digest
                            .setPacketFileDigest(ByteBuffer.wrap("placeholder".getBytes()))
                            .build()
            );
        }
    }

    public static class ForkByIndexFn extends DoFn<List<PrioDataSharePacket>, PrioDataSharePacket> {
        private final int index;
        public ForkByIndexFn(int index) {
            this.index = index;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (index < c.element().size()) {
                c.output(c.element().get(index));
            }
        }
    }

}