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
package edu.iu.dsc.tws.comms.dfw.io.allreduce;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.TreeBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.reduce.BaseReduceBatchFinalReceiver;

public class AllReduceBatchFinalReceiver extends BaseReduceBatchFinalReceiver {
  private TreeBroadcast reduceReceiver;

  public AllReduceBatchFinalReceiver(ReduceFunction reduce, TreeBroadcast receiver) {
    super(reduce);
    this.reduceReceiver = receiver;
  }

  @Override
  protected boolean handleFinished(int task, Object value) {
    return reduceReceiver.send(task, value, 0);
  }

  @Override
  protected boolean sendSyncForward(boolean needsFurtherProgress, int target) {
    if (reduceReceiver.send(target, new byte[0], MessageFlags.END)) {
      isSyncSent.put(target, true);
    } else {
      return true;
    }
    return needsFurtherProgress;
  }
}
