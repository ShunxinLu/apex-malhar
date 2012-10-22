/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.stream;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.OperatorConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a HashMap in stream <b>data</b> and just emits its keys, keyvals, vals. Used for breaking up a HashMap<p>
 * <br>
 * <br>
 * <b>Port Interface</b><br>
 * <b>data</b>: expects HashMap<K,V><br>
 * <b>key</b>: emits K<br>
 * <b>keyval</b>: emits HashMap<K,V>(1)<br>
 * <b>val</b>: emits V<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * <b>Compile time checks are</b>:<br>
 * None
 * <b>Benchmarks</b>
 * TBD
 * <br>
 *
 * @author amol
 */
public class HashMapToKey<K, V> extends BaseOperator
{
  public final transient DefaultInputPort<HashMap<K, V>> data = new DefaultInputPort<HashMap<K, V>>(this)
  {
    @Override
    public void process(HashMap<K, V> tuple)
    {
      for (Map.Entry<K, V> e: tuple.entrySet()) {
        if (key.isConnected()) {
          key.emit(e.getKey());
        }
        if (val.isConnected()) {
          val.emit(e.getValue());
        }
        if (keyval.isConnected()) {
          HashMap<K,V> otuple = new HashMap<K,V>(1);
          otuple.put(e.getKey(), e.getValue());
          keyval.emit(otuple);
        }
      }
    }
  };
  public final transient DefaultOutputPort<K> key = new DefaultOutputPort<K>(this);
  public final transient DefaultOutputPort<HashMap<K, V>> keyval = new DefaultOutputPort<HashMap<K, V>>(this);
  public final transient DefaultOutputPort<V> val = new DefaultOutputPort<V>(this);
}
