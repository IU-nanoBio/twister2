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
package edu.iu.dsc.tws.examples.task.streaming;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.task.BenchTaskWorker;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkConstants;
import edu.iu.dsc.tws.examples.utils.bench.BenchmarkUtils;
import edu.iu.dsc.tws.examples.utils.bench.Timing;
import edu.iu.dsc.tws.examples.verification.ResultsVerifier;
import edu.iu.dsc.tws.examples.verification.comparators.IntArrayComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IntComparator;
import edu.iu.dsc.tws.examples.verification.comparators.IteratorComparator;
import edu.iu.dsc.tws.examples.verification.comparators.TupleComparator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.api.typed.KeyedGatherCompute;

/**
 * todo keyedgather for streaming is not applicable. This will be removed,
 * or used only with windowing
 */
public class STKeyedGatherExample extends BenchTaskWorker {

  private static final Logger LOG = Logger.getLogger(STKeyedGatherExample.class.getName());

  @Override
  public TaskGraphBuilder buildTaskGraph() {
    List<Integer> taskStages = jobParameters.getTaskStages();
    int sourceParallelism = taskStages.get(0);
    int sinkParallelism = taskStages.get(1);
    DataType keyType = DataType.INTEGER_ARRAY;
    DataType dataType = DataType.INTEGER_ARRAY;
    String edge = "edge";
    BaseSource g = new SourceTask(edge, true);
    ISink r = new KeyedGatherSinkTask();
    taskGraphBuilder.addSource(SOURCE, g, sourceParallelism);
    computeConnection = taskGraphBuilder.addSink(SINK, r, sinkParallelism);
    computeConnection.keyedGather(SOURCE, edge, keyType, dataType);
    return taskGraphBuilder;
  }

  protected static class KeyedGatherSinkTask extends KeyedGatherCompute<Integer, int[]>
      implements ISink {
    private static final long serialVersionUID = -254264903510284798L;
    private ResultsVerifier<int[], Iterator<Tuple<Integer, int[]>>> resultsVerifier;
    private boolean verified = true;
    private boolean timingCondition;

    private int count = 0;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      this.timingCondition = getTimingCondition(SINK, context);
      resultsVerifier = new ResultsVerifier<>(inputDataArray, (ints, args) -> {
        int sinksCount = ctx.getTasksByName(SINK).size();
        Set<Integer> taskIds = ctx.getTasksByName(SOURCE).stream()
            .map(TaskInstancePlan::getTaskIndex)
            .filter(i -> i % sinksCount == ctx.taskIndex())
            .collect(Collectors.toSet());

        List<Tuple<Integer, int[]>> finalOutput = new ArrayList<>();

        taskIds.forEach(key -> {
          finalOutput.add(new Tuple<>(key, ints));
        });

        return finalOutput.iterator();
      }, new IteratorComparator<>(
          new TupleComparator<>(
              IntComparator.getInstance(),
              IntArrayComparator.getInstance()
          )
      ));
      receiversInProgress.incrementAndGet();
    }

    @Override
    public boolean keyedGather(Iterator<Tuple<Integer, int[]>> data) {
      count++;
      if (count > jobParameters.getWarmupIterations()) {
        Timing.mark(BenchmarkConstants.TIMING_MESSAGE_RECV, this.timingCondition);
      }

      if (count == jobParameters.getTotalIterations()) {
        LOG.info(String.format("%d received keyed-gather %d",
            context.getWorkerId(), context.taskId()));
        Timing.mark(BenchmarkConstants.TIMING_ALL_RECV, this.timingCondition);
        BenchmarkUtils.markTotalAndAverageTime(resultsRecorder, this.timingCondition);
        resultsRecorder.writeToCSV();
        receiversInProgress.decrementAndGet();
      }
      this.verified = verifyResults(resultsVerifier, data, null, verified);
      return true;
    }
  }
}
