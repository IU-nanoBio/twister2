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
package edu.iu.dsc.tws.executor;

import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraph;
import edu.iu.dsc.tws.task.taskgraphbuilder.TaskEdge;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public interface IExecutor {
  /**
   * Schedule and execution
   * @param taskGraph the task graph
   * @param taskSchedule the task schedule
   * @return the execution created and null if nothing to execute
   */
  Execution schedule(DataflowTaskGraph<ITask, TaskEdge> taskGraph, TaskSchedulePlan taskSchedule);

  /**
   * Wait for an execution to finish
   * @param execution the execution to wait for
   */
  void wait(Execution execution);

  /**
   * Close an already running execution
   * @param execution the execution to be stopped
   */
  void stop(Execution execution);
}
