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

package org.apache.apex.examples.innerjoin;

import com.google.common.collect.Multimap;
import org.apache.apex.malhar.lib.join.POJOInnerJoinOperator;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowedMergeOperator;
import org.apache.apex.malhar.lib.window.accumulation.PojoInnerJoin;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedMergeOperatorImpl;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import java.util.List;
import java.util.Set;

@ApplicationAnnotation(name = "InnerJoinExample")
/**
 * @since 3.7.0
 */
public class InnerJoinApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // SalesEvent Generator
    POJOGenerator salesGenerator = dag.addOperator("Input1", new POJOGenerator());
    // ProductEvent Generator
    POJOGenerator productGenerator = dag.addOperator("Input2", new POJOGenerator());
    productGenerator.setSalesEvent(false);

    // Inner join Operator
    WindowedMergeOperatorImpl<Object, Object, List<Set<Object>>, List<List<Object>>> join
        = dag.addOperator("Join", new WindowedMergeOperatorImpl<Object, Object, List<Set<Object>>, List<List<Object>>>());
    join.setWindowOption(new WindowOption.GlobalWindow());
    join.setAccumulation(new PojoInnerJoin(POJOGenerator.OutputEvent.class, new String[]{"productId"}, new String[]{"productId"}));
    join.setDataStorage(new InMemoryWindowedStorage<List<Set<Object>>>());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    // Streams
    dag.addStream("SalesToJoin", salesGenerator.output, join.input);
    dag.addStream("ProductToJoin", productGenerator.output, join.input2);
    dag.addStream("JoinToConsole", join.output, output.input);
  }
}
