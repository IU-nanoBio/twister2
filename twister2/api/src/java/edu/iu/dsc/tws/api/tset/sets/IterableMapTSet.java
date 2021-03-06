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

package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.IterableMapOp;
import edu.iu.dsc.tws.common.config.Config;

/**
 * Iterable Map Set
 *
 * @param <I> Input type
 * @param <O> Output type
 */
public class IterableMapTSet<I, O> extends BatchBaseTSet<O> {
  private BaseTLink<I> parent;

  private IterableMapFunction<I, O> mapFn;

  public IterableMapTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<I> parent,
                         IterableMapFunction<I, O> mapFunc) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = 1;
    this.name = "imap-" + parent.getName();
  }

  public IterableMapTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<I> parent,
                         IterableMapFunction<I, O> mapFunc, int parallelism) {
    super(cfg, tSetEnv);
    this.parent = parent;
    this.mapFn = mapFunc;
    this.parallel = parallelism;
    this.name = "imap-" + parent.getName();
  }

  /**
   * The Input of the mFn is the output of the current tSet
   */
  public <O1> IterableMapTSet<O, O1> map(IterableMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.map(mFn);
  }

  public <O1> IterableFlatMapTSet<O, O1> flatMap(IterableFlatMapFunction<O, O1> mFn) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.flatMap(mFn);
  }

  public SinkTSet<O> sink(Sink<O> sink) {
    DirectTLink<O> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.sink(sink);
  }

  @SuppressWarnings("unchecked")
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);
    int p = calculateParallelism(parent);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addCompute(generateName("i-map",
            parent), new IterableMapOp<>(mapFn, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public IterableMapTSet<I, O> setName(String n) {
    this.name = n;
    return this;
  }
}
