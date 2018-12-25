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
package edu.iu.dsc.tws.comms.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.core.TaskPlan;

/**
 * Routing is done in a fixed ring. This is an efficient topology to be used with operations such
 * asl AllReduce, AllGather.  0 -> 1 -> 2 ->3 -> 4 -> 0
 */
public class RingRouter {

  private static final Logger LOG = Logger.getLogger(RingRouter.class.getName());
  // the task plan
  private Map<Integer, List<Integer>> receiveTasks;
  private Set<Integer> receiveExecutors;
  private Map<Integer, Integer> destinationIdentifiers;
  private Map<Integer, Set<Integer>> sendExternalTasksPartial;
  private TaskPlan taskPlan;
  private int mainTask;

  private Map<Integer, Set<Integer>> sendExternalTasks;
  private Map<Integer, Set<Integer>> sendInternalTasks;

  /**
   * Ring Router
   */
  public RingRouter(TaskPlan plan, Set<Integer> srscs, Set<Integer> dests) {
    this.taskPlan = plan;

    Set<Integer> thisExecutorTasks = plan.getChannelsOfExecutor(plan.getThisExecutor());
    FixedRing fixedRing = new FixedRing(plan, srscs, dests);
    /*
      Tasks belonging to this operation and in the same executor
    */
    Set<Integer> thisExecutorTasksOfOperation = new HashSet<>();
    for (int t : thisExecutorTasks) {
      if (dests.contains(t) || srscs.contains(t)) {
        thisExecutorTasksOfOperation.add(t);
      }
    }

    LOG.fine(String.format("%d Executor Tasks: %s", plan.getThisExecutor(),
        thisExecutorTasksOfOperation.toString()));

    this.destinationIdentifiers = new HashMap<>();
    // construct the map of receiving ids
    this.receiveTasks = new HashMap<>();

    // now lets construct the downstream tasks
    sendExternalTasksPartial = new HashMap<>();
    sendInternalTasks = new HashMap<>();
    sendExternalTasks = new HashMap<>();

    // now lets construct the receive tasks tasks
    receiveExecutors = new HashSet<>();
    for (int t : thisExecutorTasksOfOperation) {
      List<Integer> recv = new ArrayList<>();

      Node node = fixedRing.get(t);
      if (node != null) {
        mainTask = node.getTaskId();
        LOG.fine(String.format("%d main task: %d", plan.getThisExecutor(), mainTask));
        // this is the only task that receives messages
        for (int k : node.getRemoteChildrenIds()) {
          receiveExecutors.add(plan.getExecutorForChannel(k));
        }
        recv.addAll(node.getAllChildrenIds());
        receiveTasks.put(t, new ArrayList<>(recv));

        // this task is connected to others and they send the message to this task
        List<Integer> directChildren = node.getDirectChildren();
        for (int child : directChildren) {
          Set<Integer> sendTasks = new HashSet<>();
          sendTasks.add(t);
          sendInternalTasks.put(child, sendTasks);
          destinationIdentifiers.put(child, t);
        }


        // now lets calculate the external send tasks of the main task
        Node downStream = node.getParent();
        if (downStream == null) {
          throw new IllegalStateException("Downstream node cannot be null, ring communication "
              + "should always have a downstream element");
        }
        if (downStream != null) {
          Set<Integer> sendTasks = new HashSet<>();
          sendTasks.add(downStream.getTaskId());
          sendExternalTasksPartial.put(t, sendTasks);
          destinationIdentifiers.put(t, downStream.getTaskId());
        }
      }

    }
  }
}
