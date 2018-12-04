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
package edu.iu.dsc.tws.api.tset.impl;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;
import edu.iu.dsc.tws.common.config.Config;

public class GroupedTSet<T> extends BaseTSet<T> {
  private PartitionFunction<T> partitioner;

  public GroupedTSet(Config cfg, TaskGraphBuilder bldr) {
    super(cfg, bldr);
  }

  public KeyedReduceTSet<T> keyedReduce(ReduceFunction<T> reduceFn) {
    KeyedReduceTSet<T> reduce = new KeyedReduceTSet<>(config, builder, this, reduceFn, partitioner);
    children.add(reduce);
    return reduce;
  }

  public KeyedPartitionTSet<T> keyedPartition() {
    KeyedPartitionTSet<T> partition = new KeyedPartitionTSet<>(config, builder, this, partitioner);
    children.add(partition);
    return partition;
  }

  public KeyedGatherTSet<T> keyedGather() {
    KeyedGatherTSet<T> gather = new KeyedGatherTSet<>(config, builder, this, partitioner);
    children.add(gather);
    return gather;
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  protected Op getOp() {
    return Op.GROUPED;
  }
}
