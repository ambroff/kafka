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

import org.apache.kafka.common.message.ControlledShutdownSkipSafetyCheckRequestData;
import org.apache.kafka.common.message.ControlledShutdownSkipSafetyCheckResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class ControlledShutdownSkipSafetyCheckRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<ControlledShutdownSkipSafetyCheckRequest> {
        private final ControlledShutdownSkipSafetyCheckRequestData data;

        public Builder(ControlledShutdownSkipSafetyCheckRequestData data, short desiredVersion) {
            super(ApiKeys.CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK, desiredVersion);
            this.data = data;
        }

        @Override
        public ControlledShutdownSkipSafetyCheckRequest build(short version) {
            return new ControlledShutdownSkipSafetyCheckRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ControlledShutdownSkipSafetyCheckRequestData data;
    private final short version;

    public ControlledShutdownSkipSafetyCheckRequest(ControlledShutdownSkipSafetyCheckRequestData data, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK, version);
        this.data = data;
        this.version = version;
    }

    public ControlledShutdownSkipSafetyCheckRequest(Struct struct, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK, version);
        this.data = new ControlledShutdownSkipSafetyCheckRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ControlledShutdownSkipSafetyCheckResponseData data = new ControlledShutdownSkipSafetyCheckResponseData()
            .setErrorCode(Errors.forException(e).code());
        return new ControlledShutdownSkipSafetyCheckResponse(data);
    }

    public static ControlledShutdownSkipSafetyCheckRequest parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownSkipSafetyCheckRequest(
            ApiKeys.CONTROLLED_SHUTDOWN_SKIP_SAFETY_CHECK.parseRequest(version, buffer), version);
    }
}
