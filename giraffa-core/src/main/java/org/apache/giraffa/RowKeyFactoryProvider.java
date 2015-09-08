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

import static org.apache.giraffa.GiraffaConfiguration.GRFA_CACHING_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_CACHING_KEY;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROWKEY_FACTORY_DEFAULT;
import static org.apache.giraffa.GiraffaConfiguration.GRFA_ROWKEY_FACTORY_KEY;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class RowKeyFactoryProvider {

  private static Class<? extends RowKeyFactory> RowKeyFactoryClass;
  private static boolean caching;

  public static RowKeyFactory createFactory(Configuration conf,
                                            GiraffaProtocol service) {
    registerFactory(conf);
    RowKeyFactory fact = ReflectionUtils.newInstance(RowKeyFactoryClass, conf);
    fact.setService(service);
    return fact;
  }

  public static RowKeyFactory createFactory(GiraffaFileSystem grfs) {
    return createFactory(grfs.getConf(), grfs.grfaClient.getNamespaceService());
  }

  public static Class<? extends RowKeyFactory> getFactoryClass() {
    return RowKeyFactoryClass;
  }

  public static boolean isCaching() {
    return caching;
  }

  private static synchronized void registerFactory(Configuration conf) {
    if (RowKeyFactoryClass == null) {
      RowKeyFactoryClass = conf.getClass(GRFA_ROWKEY_FACTORY_KEY,
          GRFA_ROWKEY_FACTORY_DEFAULT, RowKeyFactory.class);
      caching = conf.getBoolean(GRFA_CACHING_KEY, GRFA_CACHING_DEFAULT);
    }
  }
}
