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

import java.util.List;
import java.util.Set;

import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.accumulation.PojoInnerJoin;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedMergeOperatorImpl;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;


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
    salesGenerator.setMaxProductId(10);
    // ProductEvent Generator
    POJOGenerator productGenerator = dag.addOperator("Input2", new POJOGenerator());
    productGenerator.setMaxProductId(5);
    productGenerator.setSalesEvent(false);

    // Inner join Operator
    WindowedMergeOperatorImpl<POJOGenerator.SalesEvent, POJOGenerator.ProductEvent, List<Set<Object>>, List<List<Object>>> joinOp
        = new WindowedMergeOperatorImpl<>();
    joinOp.setDataStorage(new InMemoryWindowedStorage<List<Set<Object>>>());
    PojoInnerJoin accu = new PojoInnerJoin(POJOGenerator.SalesEvent.class, new String[]{"productId", "productCategory", "timestamp"}, new String[]{"productId", "productCategory", "timestamp"});
    joinOp.setAccumulation(accu);
    joinOp.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    joinOp.setWindowOption(new WindowOption.GlobalWindow());
    joinOp.setTriggerOption(new TriggerOption().withEarlyFiringsAtEvery(10));

    dag.addOperator("Join", joinOp);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    // Streams
    dag.addStream("SalesToJoin", salesGenerator.outputsales, joinOp.input);
    dag.addStream("ProductToJoin", productGenerator.outputproduct, joinOp.input2);
    dag.addStream("JoinToConsole", joinOp.output, output.input);

    // Setting tuple class properties to the ports of join operator
    dag.setInputPortAttribute(joinOp.input, Context.PortContext.TUPLE_CLASS, POJOGenerator.SalesEvent.class);
    dag.setInputPortAttribute(joinOp.input2, Context.PortContext.TUPLE_CLASS, POJOGenerator.ProductEvent.class);
    dag.setOutputPortAttribute(joinOp.output,Context.PortContext.TUPLE_CLASS, POJOGenerator.SalesEvent.class);
  }
}
