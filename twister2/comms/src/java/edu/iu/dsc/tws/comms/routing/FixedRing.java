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

import java.util.HashMap;
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
  private Map<Integer, RingNode> ring;

  public FixedRing(TaskPlan plan, Set<Integer> sources, Set<Integer> destinations) {
//    for (Integer integer : plan.get) {
//
//    }
    ring = new HashMap<>();

  }

  public RingNode get(int t) {
    return null;
  }
}
