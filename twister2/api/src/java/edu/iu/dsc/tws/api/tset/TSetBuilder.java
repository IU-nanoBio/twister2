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
package edu.iu.dsc.tws.api.tset;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.BatchSourceTSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingSourceTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

/**
 * The builder for creating a tset graph.
 */
public final class TSetBuilder {
  /**
   * List of sources added to this builder
   */
  private List<BaseTSet<?>> sources;

  /**
   * Operation mode
   */
  private OperationMode opMode = OperationMode.STREAMING;

  /**
   * Configuration
   */
  private Config config;

  /**
   * The task builder tobe used to build the task graph
   */
  private TaskGraphBuilder builder;

  private TSetBuilder(Config cfg) {
    this.config = cfg;
    this.sources = new ArrayList<>();
    this.builder = TaskGraphBuilder.newBuilder(cfg);
  }

  /**
   * This method is used to create a new builder
   *
   * @param cfg configuration
   * @return the builder
   */
  public static TSetBuilder newBuilder(Config cfg) {
    return new TSetBuilder(cfg);
  }

  public OperationMode getOpMode() {
    return opMode;
  }

  public TSetBuilder setMode(OperationMode mode) {
    this.opMode = mode;
    return this;
  }

  public <T> BatchSourceTSet<T> createBatchSource(Source<T> source, int parallelism,
                                                  TSetEnv tSetEnv) {
    builder.setMode(opMode);
    BatchSourceTSet<T> tSourceTSet = new BatchSourceTSet<>(config, tSetEnv, source, parallelism);
    sources.add(tSourceTSet);
    return tSourceTSet;
  }

  public <T> StreamingSourceTSet<T> createStreamingSource(Source<T> source, int parallelism,
                                                          TSetEnv tSetEnv) {
    builder.setMode(opMode);
    StreamingSourceTSet<T> tSourceTSet = new StreamingSourceTSet<>(config, tSetEnv,
        source, parallelism);
    sources.add(tSourceTSet);
    return tSourceTSet;
  }

  public DataFlowTaskGraph build() {
    builder.setMode(opMode);
    for (BaseTSet<?> set : sources) {
      set.build();
    }
    return builder.build();
  }

  public TaskGraphBuilder getTaskGraphBuilder() {
    return builder;
  }
}
