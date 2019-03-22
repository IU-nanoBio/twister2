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
package edu.iu.dsc.tws.master.dashclient.messages;

import java.util.List;
import java.util.logging.Logger;

public class ScaleWorkers {
  private static final Logger LOG = Logger.getLogger(ScaleWorkers.class.getName());

  private int change;
  private int numberOfWorkers;
  private List<Integer> killedWorkers;

  public ScaleWorkers() {
  }

  public ScaleWorkers(int change, int numberOfWorkers, List<Integer> killedWorkers) {
    this.change = change;
    this.numberOfWorkers = numberOfWorkers;
    this.killedWorkers = killedWorkers;
  }

  public void setChange(int change) {
    this.change = change;
  }

  public int getChange() {
    return change;
  }

  public List<Integer> getKilledWorkers() {
    return killedWorkers;
  }

  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }

  public void setKilledWorkers(List<Integer> killedWorkers) {
    this.killedWorkers = killedWorkers;
  }
}
