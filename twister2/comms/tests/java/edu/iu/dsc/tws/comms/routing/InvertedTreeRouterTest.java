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

import java.util.Set;

import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import static edu.iu.dsc.tws.comms.routing.RoutingTestUtils.createTaskPlan;
import static edu.iu.dsc.tws.comms.routing.RoutingTestUtils.destinations;

public class InvertedTreeRouterTest {
  @Test
  public void testUniqueTrees() {
    TaskPlan p = createTaskPlan(256, 1, 0);

    Set<Integer> s = destinations(256, 1);
    InvertedBinaryTreeRouter router = new InvertedBinaryTreeRouter(Config.newBuilder().build(),
        p, 0,  s, 0);
  }
}
