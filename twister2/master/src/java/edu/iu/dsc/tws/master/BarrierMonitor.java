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
package edu.iu.dsc.tws.master;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network;

public class BarrierMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(BarrierMonitor.class.getName());

  private int numberOfWorkers;
  private HashMap<Integer, RequestID> waitList;
  private RRServer rrServer;

  public BarrierMonitor(int numberOfWorkers, RRServer rrServer) {
    this.numberOfWorkers = numberOfWorkers;
    this.rrServer = rrServer;
    waitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID requestID, int workerId, Message message) {

    if (message instanceof Network.BarrierRequest) {
      Network.BarrierRequest barrierRequest = (Network.BarrierRequest) message;
      LOG.info("BarrierRequest message received:\n" + barrierRequest);

      waitList.put(barrierRequest.getWorkerID(), requestID);

      if (waitList.size() == numberOfWorkers) {
        sendBarrierResponseToWaitList();
      }

    } else {
      LOG.log(Level.SEVERE, "Un-known message received: " + message);
    }
  }

  /**
   * First, we copy the waitList to another
   * This is to prevent that after some responses are sent,
   * new barrier requests may arrive, before finishing up the response sends
   */
  private void sendBarrierResponseToWaitList() {

    HashMap<Integer, RequestID> waitListCopy = copyWaitList();
    waitList.clear();

    for (Map.Entry<Integer, RequestID> entry: waitListCopy.entrySet()) {
      Network.BarrierResponse response = Network.BarrierResponse.newBuilder()
          .setWorkerID(entry.getKey())
          .build();

      rrServer.sendResponse(entry.getValue(), response);
      LOG.info("BarrierResponse sent:\n" + response);
    }
  }

  private HashMap<Integer, RequestID> copyWaitList() {

    HashMap<Integer, RequestID> waitListCopy = new HashMap<>(waitList.size());
    for (Map.Entry<Integer, RequestID> entry: waitList.entrySet()) {
      waitListCopy.put(entry.getKey(), entry.getValue());
    }

    return waitListCopy;
  }


}
