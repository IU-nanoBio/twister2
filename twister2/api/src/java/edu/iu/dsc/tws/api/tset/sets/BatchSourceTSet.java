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

import java.util.Random;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.fn.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.ops.SourceOp;
import edu.iu.dsc.tws.common.config.Config;

public class BatchSourceTSet<T> extends BatchBaseTSet<T> {
  private Source<T> source;

  public BatchSourceTSet(Config cfg, TSetEnv tSetEnv, Source<T> src, int parallelism) {
    super(cfg, tSetEnv);
    this.source = src;
    this.name = "source-" + new Random(System.nanoTime()).nextInt(10);
    this.parallel = parallelism;
  }

  public <P> IterableMapTSet<T, P> map(IterableMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.map(mapFn);
  }

  public <P> IterableFlatMapTSet<T, P> flatMap(IterableFlatMapFunction<T, P> mapFn) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.flatMap(mapFn);
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    DirectTLink<T> direct = new DirectTLink<>(config, tSetEnv, this);
    children.add(direct);
    return direct.sink(sink);
  }

  @Override
  public boolean baseBuild() {
    tSetEnv.getTSetBuilder().getTaskGraphBuilder().
        addSource(getName(), new SourceOp<T>(source), parallel);
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public BatchSourceTSet<T> setName(String n) {
    this.name = n;
    return this;
  }
}
