/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.sample.complete;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

/**
 * Testing the AutoComplete Application
 */
public class AutoCompleteTest
{
  private static final Logger logger = LoggerFactory.getLogger(AutoCompleteTest.class);

  @Test
  public void AutoCompleteTest() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new AutoComplete(), conf);
    LocalMode.Controller lc = lma.getController();
    long start = System.currentTimeMillis();
    lc.run(100000);
    long end = System.currentTimeMillis();
    long time = end - start;
    logger.info("Test used " + time + " ms");
  }
}
