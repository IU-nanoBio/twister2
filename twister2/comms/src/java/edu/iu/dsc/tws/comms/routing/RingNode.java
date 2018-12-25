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
import java.util.Collection;
import java.util.List;

public class RingNode {

  /**
   * The main task for this node
   */
  private int mainTaskId = -1;

  private List<Integer> directChildren = new ArrayList<>();

  public int[] getRemoteChildrenIds() {
    return null;
  }

  public Collection<? extends Integer> getAllChildrenIds() {
    return null;
  }

  public List<Integer> getDirectChildren() {
    return null;
  }

  public Node getDownStream() {
    return null;
  }

  public int getMainTaskId() {
    return mainTaskId;
  }

  public void setMainTaskId(int mainTaskId) {
    this.mainTaskId = mainTaskId;
  }
}
