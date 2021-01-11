/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.LiControlledShutdownSkipSafetyCheckResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class LiControlledShutdownSkipSafetyCheckResponse extends AbstractResponse {
    /**
     * Possible error codes:
     * <p>
     * UNKNOWN(-1) (this is because IllegalStateException may be thrown in `KafkaController.shutdownBroker`, it would be good to improve this)
     * STALE_CONTROLLER_EPOCH(11)
     */
    private final LiControlledShutdownSkipSafetyCheckResponseData data;

    public LiControlledShutdownSkipSafetyCheckResponse(LiControlledShutdownSkipSafetyCheckResponseData data) {
        this.data = data;
    }

    public LiControlledShutdownSkipSafetyCheckResponse(Struct struct, short version) {
        this.data = new LiControlledShutdownSkipSafetyCheckResponseData(struct, version);
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(error(), 1);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public LiControlledShutdownSkipSafetyCheckResponseData data() {
        return data;
    }

    public static LiControlledShutdownSkipSafetyCheckResponse parse(ByteBuffer buffer, short version) {
        return new LiControlledShutdownSkipSafetyCheckResponse(
            ApiKeys.LI_CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK.parseResponse(version, buffer),
            version);
    }

    public static LiControlledShutdownSkipSafetyCheckResponse prepareResponse(Errors error) {
        LiControlledShutdownSkipSafetyCheckResponseData data = new LiControlledShutdownSkipSafetyCheckResponseData();
        data.setErrorCode(error.code());
        return new LiControlledShutdownSkipSafetyCheckResponse(data);
    }
}
