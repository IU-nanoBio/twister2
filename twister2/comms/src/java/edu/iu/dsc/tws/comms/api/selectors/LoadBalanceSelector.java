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
package edu.iu.dsc.tws.comms.api.selectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.DestinationSelector;

public class LoadBalanceSelector implements DestinationSelector {
  private static final Logger LOG = Logger.getLogger(
      LoadBalanceSelector.class.getName());

  private Map<Integer, List<Integer>> destination = new HashMap<>();

  private Map<Integer, Integer> destinationIndexes = new HashMap<>();

  private Map<Integer, Map<Integer, Integer>> invertedIndexes = new HashMap<>();

  @Override
  public void prepare(Communicator comm, Set<Integer> sources, Set<Integer> destinations) {
    for (int s : sources) {
      ArrayList<Integer> destList = new ArrayList<>(destinations);
      destination.put(s, destList);
      destinationIndexes.put(s, 0);
      HashMap<Integer, Integer> value = new HashMap<>();
      invertedIndexes.put(s, value);
      for (int i = 0; i < destinations.size(); i++) {
        value.put(destList.get(i), i);
      }
    }
  }

  @Override
  public int next(int source, Object key, Object data) {
    return next(source, data);
  }

  private int increment(int current, int max) {
    if (current == max - 1) {
      return 0;
    } else {
      return current + 1;
    }
  }

  @Override
  public int next(int source, Object data) {
    int destIndex = destinationIndexes.get(source);
    List<Integer> destinations = destination.get(source);
    int next = increment(destIndex, destinations.size());
    return destinations.get(next);

  }

  public void commit(int source, int dest) {
    Map<Integer, Integer> invertedDests = invertedIndexes.get(source);
    int index = invertedDests.get(dest);

    destinationIndexes.put(source, index);
  }
}
