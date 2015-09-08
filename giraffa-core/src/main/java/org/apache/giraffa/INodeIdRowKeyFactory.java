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

import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class INodeIdRowKeyFactory extends RowKeyFactory {

  @Override // RowKeyFactory
  public INodeIdRowKey getRowKey(String src) throws IOException {
    return getRowKey(src, -1);
  }

  @Override // RowKeyFactory
  public INodeIdRowKey getRowKey(String src, long inodeId) throws IOException {
    Path path = new Path(src);
    if (path.isRoot()) {
      return INodeIdRowKey.ROOT;
    }
    INodeIdRowKey parent = getRowKey(path.getParent().toString());
    if (inodeId < 0) {
      inodeId = getService().getFileId(parent.getKey(), src);
    }
    return getRowKey(parent, src, inodeId);
  }

  @Override // RowKeyFactory
  public INodeIdRowKey getRowKey(String src, byte[] bytes) {
    return new INodeIdRowKey(src, bytes);
  }

  private INodeIdRowKey getRowKey(INodeIdRowKey parent, String src, long id) {
    if (id < 0) {
      return getRowKey(src, INodeIdRowKey.EMPTY);
    } else {
      return new INodeIdRowKey(src, parent, id);
    }
  }
}
