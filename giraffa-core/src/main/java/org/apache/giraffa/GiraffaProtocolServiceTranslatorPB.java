/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraffa;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import org.apache.giraffa.GiraffaProtos.GetFileIdRequestProto;
import org.apache.giraffa.GiraffaProtos.GiraffaProtocolService.BlockingInterface;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolTranslatorPB;
import org.apache.hadoop.ipc.ProtobufHelper;

import java.io.IOException;

public class GiraffaProtocolServiceTranslatorPB
    extends ClientNamenodeProtocolTranslatorPB implements GiraffaProtocol {

  private final BlockingInterface service;

  public GiraffaProtocolServiceTranslatorPB(BlockingInterface service) {
    super(new GiraffaProtocolServiceAdapterPB(service));
    this.service = service;
  }

  @Override // FileIdProtocol
  public long getFileId(byte[] parentKey, String src) throws IOException {
    GetFileIdRequestProto req = GetFileIdRequestProto.newBuilder()
        .setParentKey(ByteString.copyFrom(parentKey))
        .setSrc(src)
        .build();
    try {
      return service.getFileId(null, req).getFileId();
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
}