//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package edu.iu.dsc.tws.api.tset.link.streaming;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingMapTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class StreamingReduceTLink<T> extends BaseTLink<T> {
  private ReduceFunction<T> reduceFn;

  private BaseTSet<T> parent;

  public StreamingReduceTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt,
                              ReduceFunction<T> rFn) {
    super(cfg, tSetEnv);
    this.reduceFn = rFn;
    this.parent = prnt;
    this.name = "reduce-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  public <P> StreamingMapTSet<P, T> map(MapFunction<T, P> mapFn) {
    StreamingMapTSet<P, T> set = new StreamingMapTSet<P, T>(config, tSetEnv, this,
        mapFn, 1);
    children.add(set);
    return set;
  }

  public <P> StreamingFlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    StreamingFlatMapTSet<P, T> set = new StreamingFlatMapTSet<P, T>(config, tSetEnv, this,
        mapFn, 1);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink,
        1);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    DataType dataType = TSetUtils.getDataType(getType());

    connection.reduce(parent.getName(), Constants.DEFAULT_EDGE,
        new ReduceOpFunction<>(getReduceFn()), dataType);
  }

  public ReduceFunction<T> getReduceFn() {
    return reduceFn;
  }

  @Override
  public int overrideParallelism() {
    return 1;
  }

  @Override
  public StreamingReduceTLink<T> setName(String n) {
    super.setName(n);
    return this;
  }
}
