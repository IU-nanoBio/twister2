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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.comms.core.TaskPlan;

/**
 * Ring topology with fixed pattern.
 */
public class FixedRing {

  // the task plan
  private TaskPlan taskPlan;
  /**
   * A circular list which maps executor id's
   */
  private Map<Integer, Node> ring;

  public FixedRing(TaskPlan plan, Set<Integer> sources, Set<Integer> destinations) {
    this.taskPlan = plan;
    ring = new HashMap<>();
    List<Integer> tasks;
    // create nodes for each executor and connect them
    Iterator<Integer> iter = plan.getAllExecutors().iterator();
    while (iter.hasNext()) {
      int executor = iter.next();
      tasks = new ArrayList<>(plan.getChannelsOfExecutor(executor));
      Collections.sort(tasks);
      int group = plan.getGroupOfExecutor(executor);
      int mainTaskId = -1;
      // set the smallest task in the group as the main task
      if (tasks != null && tasks.size() > 0) {
        mainTaskId = tasks.get(0);
        tasks.remove(mainTaskId);
      }

      if (mainTaskId == -1) {
        throw new IllegalStateException("The main task ID must be set");
      }
      Node node = new Node(mainTaskId, group);
      node.addDirectChildren(tasks);

      ring.put(executor, node);
    }

    //Now to connect the ring
    iter = plan.getAllExecutors().iterator();
    int ringSize = plan.getAllExecutors().size();
    while (iter.hasNext()) {
      int executor = iter.next();
      int child =  (executor == 0) ? ringSize - 1 : executor - 1;
      int parent = (executor + 1) % ringSize;
      ring.get(executor).setParent(ring.get(parent));
      ring.get(executor).addChild(ring.get(child));

    }

  }

  public Node get(int t) {
    return null;
  }
}
