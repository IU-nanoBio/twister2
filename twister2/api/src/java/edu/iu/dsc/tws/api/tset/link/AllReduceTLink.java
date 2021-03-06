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

package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.ops.ReduceOpFunction;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

/**
 * Represent a data set create by a all reduce opration
 *
 * @param <T> type of data
 */
public class AllReduceTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink<T> {
  private ReduceFunction<T> reduceFn;

  private BaseTSet<T> parent;

  public AllReduceTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt, ReduceFunction<T> rFn) {
    super(cfg, tSetEnv);
    this.reduceFn = rFn;
    this.parent = prnt;
    this.name = "all-reduce-" + parent.getName();
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn, int parallelism) {
    MapTSet<P, T> set = new MapTSet<P, T>(config, tSetEnv, this, mapFn, parallelism);
    children.add(set);
    return set;
  }

  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn, int parallelism) {
    FlatMapTSet<P, T> set = new FlatMapTSet<P, T>(config, tSetEnv, this, mapFn, parallelism);
    children.add(set);
    return set;
  }

  public SinkTSet<T> sink(Sink<T> sink, int parallelism) {
    SinkTSet<T> sinkTSet = new SinkTSet<>(config, tSetEnv, this, sink, parallelism);
    children.add(sinkTSet);
    tSetEnv.run();
    return sinkTSet;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    DataType dataType = TSetUtils.getDataType(getType());

    connection.allreduce(parent.getName(), Constants.DEFAULT_EDGE,
        new ReduceOpFunction<T>(getReduceFn()), dataType);
  }

  public ReduceFunction<T> getReduceFn() {
    return reduceFn;
  }

  @Override
  public AllReduceTLink<T> setName(String n) {
    super.setName(n);
    return this;
  }
}
