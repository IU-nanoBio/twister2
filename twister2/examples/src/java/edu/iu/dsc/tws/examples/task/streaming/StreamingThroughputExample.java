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

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.TaskUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.streaming.BaseStreamCompute;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public class StreamingThroughputExample extends TaskWorker {

  private static final Logger LOG = Logger.getLogger(StreamingThroughputExample.class.getName());

  @Override
  public void execute() {
    GeneratorTask g = new GeneratorTask();
    MiddleTask m = new MiddleTask();
    ReceivingTask r = new ReceivingTask();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    builder.addSource("source", g, 4);
    ComputeConnection middle = builder.addCompute("middle", m, 4);
    middle.partition("source", "partition-edge-1", DataType.OBJECT);

    ComputeConnection sink = builder.addSink("sink", r, 4);
    sink.partition("middle", "partition-edge-2", DataType.OBJECT);

    builder.setMode(OperationMode.STREAMING);

    DataFlowTaskGraph graph = builder.build();
    TaskUtils.execute(config, workerId, graph, workerController);
  }

  private static class GeneratorTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;
    private Config config;

    private int value = 0;
    private int myIndex;
    private int worldSize;
    private int limit = 1000000;

    @Override
    public void execute() {
      if (value == limit) {
        context.write("partition-edge-1", "end");
      } else if (value < limit) {
        context.write("partition-edge-1", value + myIndex * limit);
//      LOG.log(Level.INFO, "count for source " + this.context.taskId() + " is " + value);
        value++;
      }
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.myIndex = cfg.getIntegerValue("twister2.container.id", 0);
      this.worldSize = context.getParallelism();
      super.prepare(cfg, context);
    }
  }


  public static class MiddleTask extends BaseStreamCompute {

    private static final long serialVersionUID = -254264903231284798L;

    private int count = 0;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }

    @Override
    public boolean execute(IMessage content) {
      count++;
      if (count == 10000) {
        LOG.info("Count in middle task " + context.taskId()
            + " is " + count
            + " value is" + content
        );
      }
      context.write("partition-edge-2", content.toString());


      return true;
    }

  }

  private static class ReceivingTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903511284798L;
    private Config config;

    private int count = 0;

    private TaskContext ctx;

    @Override
    public boolean execute(IMessage message) {
//      System.out.println(message.getContent() + " from Sink Task " + ctx.taskId());
      count++;
      LOG.log(Level.INFO, "count in sink " + ctx.taskId() + " is " + count);
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }

  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName(StreamingThroughputExample.class.getName());
    jobBuilder.setWorkerClass(StreamingThroughputExample.class.getName());
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}




