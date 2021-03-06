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
package edu.iu.dsc.tws.comms.dfw;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.AKeyedDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.AKeyedSerializer;
import edu.iu.dsc.tws.comms.dfw.io.KeyedDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.KeyedSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageDeSerializer;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;
import edu.iu.dsc.tws.comms.routing.InvertedBinaryTreeRouter;
import edu.iu.dsc.tws.comms.utils.OperationUtils;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class MToOneTree implements DataFlowOperation, ChannelReceiver {
  private static final Logger LOG = Logger.getLogger(MToOneTree.class.getName());

  /**
   * the source tasks
   */
  protected Set<Integer> sources;

  /**
   * The target tast
   */
  protected int destination;

  /**
   * The edge to be used
   */
  private int edgeValue;

  /**
   * The router
   */
  private InvertedBinaryTreeRouter router;

  /**
   * Final receiver
   */
  private MessageReceiver finalReceiver;

  /**
   * The local receiver
   */
  private MessageReceiver partialReceiver;

  private int index = 0;

  private int pathToUse = DataFlowContext.DEFAULT_DESTINATION;

  private ChannelDataFlowOperation delegete;
  private TaskPlan instancePlan;
  private MessageType dataType;
  private MessageType keyType;

  private boolean isKeyed = false;

  private Table<Integer, Integer, RoutingParameters> routingParamCache = HashBasedTable.create();
  private Table<Integer, Integer, RoutingParameters> partialRoutingParamCache
      = HashBasedTable.create();
  private Lock lock = new ReentrantLock();
  private Lock partialLock = new ReentrantLock();

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr,
                    MessageReceiver partialRcvr, int indx, int p) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.delegete = new ChannelDataFlowOperation(channel);
  }

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr,
                    MessageReceiver partialRcvr, int indx, int p, boolean keyed,
                    MessageType kType, MessageType dType) {
    this.index = indx;
    this.sources = sources;
    this.destination = destination;
    this.finalReceiver = finalRcvr;
    this.partialReceiver = partialRcvr;
    this.pathToUse = p;
    this.delegete = new ChannelDataFlowOperation(channel);
    this.isKeyed = keyed;
    this.keyType = kType;
    this.dataType = dType;
  }

  public MToOneTree(TWSChannel channel, Set<Integer> sources, int destination,
                    MessageReceiver finalRcvr, MessageReceiver partialRcvr) {
    this(channel, sources, destination, finalRcvr, partialRcvr, 0, 0);
  }


  /**
   * We can receive messages from internal tasks or an external task, we allways receive messages
   * to the main task of the workerId and we go from there
   */
  public boolean receiveMessage(MessageHeader header, Object object) {
    // we always receive to the main task
    // check weather this message is for a sub task
    if (!router.isLastReceiver()
        && partialReceiver != null) {
      return partialReceiver.onMessage(header.getSourceId(), DataFlowContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              DataFlowContext.DEFAULT_DESTINATION), header.getFlags(), object);
    } else {
      return finalReceiver.onMessage(header.getSourceId(), DataFlowContext.DEFAULT_DESTINATION,
          router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
              DataFlowContext.DEFAULT_DESTINATION), header.getFlags(), object);
    }
  }

  private RoutingParameters partialSendRoutingParameters(int source, int path) {
    if (partialRoutingParamCache.contains(source, path)) {
      return partialRoutingParamCache.get(source, path);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();
      // get the expected routes
      Map<Integer, Set<Integer>> internalRoutes = router.getInternalSendTasks(source);
      if (internalRoutes == null) {
        throw new RuntimeException("Un-expected message from source: " + source);
      }

      Set<Integer> sourceInternalRouting = internalRoutes.get(source);
      if (sourceInternalRouting != null) {
        routingParameters.addInternalRoutes(sourceInternalRouting);
      }

      // get the expected routes
      Map<Integer, Set<Integer>> externalRoutes =
          router.getExternalSendTasksForPartial(source);
      if (externalRoutes == null) {
        throw new RuntimeException("Un-expected message from source: " + source);
      }

      Set<Integer> sourceRouting = externalRoutes.get(source);
      if (sourceRouting != null) {
        routingParameters.addExternalRoutes(sourceRouting);
      }

      routingParameters.setDestinationId(router.destinationIdentifier(source, path));
      partialRoutingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  private RoutingParameters sendRoutingParameters(int source, int path) {
    if (routingParamCache.contains(source, path)) {
      return routingParamCache.get(source, path);
    } else {
      RoutingParameters routingParameters = new RoutingParameters();

      // get the expected routes
      Map<Integer, Set<Integer>> internalRouting = router.getInternalSendTasks(source);
      if (internalRouting == null) {
        throw new RuntimeException("Un-expected message from source: " + source);
      }

      // we are going to add source if we are the main workerId
      if (router.mainTaskOfExecutor(instancePlan.getThisExecutor(),
          DataFlowContext.DEFAULT_DESTINATION) == source) {
        routingParameters.addInteranlRoute(source);
      }

      // we should not have the route for main task to outside at this point
      Set<Integer> sourceInternalRouting = internalRouting.get(source);
      if (sourceInternalRouting != null) {
        routingParameters.addInternalRoutes(sourceInternalRouting);
      }

      routingParameters.setDestinationId(router.destinationIdentifier(source, path));
      routingParamCache.put(source, path, routingParameters);
      return routingParameters;
    }
  }

  public boolean receiveSendInternally(int source, int target, int path, int flags,
                                       Object message) {
    // check weather this is the last task
    if (router.isLastReceiver()) {
      return finalReceiver.onMessage(source, path, target, flags, message);
    } else {
      return partialReceiver.onMessage(source, path, target, flags, message);
    }
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return delegete.sendMessage(source, message, pathToUse, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    return delegete.sendMessage(source, message, target, flags,
        sendRoutingParameters(source, pathToUse));
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    return delegete.sendMessagePartial(source, message, target, flags,
        partialSendRoutingParameters(source, target));
  }


  /**
   * Initialize
   */
  public void init(Config cfg, MessageType t, TaskPlan taskPlan, int edge) {
    this.instancePlan = taskPlan;
    this.dataType = t;
    int workerId = instancePlan.getThisExecutor();
    this.edgeValue = edge;

    // we only have one path
    this.router = new InvertedBinaryTreeRouter(cfg, taskPlan,
        destination, sources, index);

    // initialize the receive
    if (this.partialReceiver != null && !router.isLastReceiver()) {
      partialReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    if (this.finalReceiver != null && router.isLastReceiver()) {
      this.finalReceiver.init(cfg, this, receiveExpectedTaskIds());
    }

    LOG.log(Level.FINE, String.format("%d reduce sources %s dest %d send tasks: %s",
        workerId, sources, destination, router.sendQueueIds()));

    Map<Integer, ArrayBlockingQueue<Pair<Object, OutMessage>>> pendingSendMessagesPerSource =
        new HashMap<>();
    Map<Integer, Queue<Pair<Object, InMessage>>> pendingReceiveMessagesPerSource
        = new HashMap<>();
    Map<Integer, Queue<InMessage>> pendingReceiveDeSerializations = new HashMap<>();
    Map<Integer, MessageSerializer> serializerMap = new HashMap<>();
    Map<Integer, MessageDeSerializer> deSerializerMap = new HashMap<>();

    Set<Integer> srcs = router.sendQueueIds();
    for (int s : srcs) {
      // later look at how not to allocate pairs for this each time
      ArrayBlockingQueue<Pair<Object, OutMessage>> pendingSendMessages =
          new ArrayBlockingQueue<Pair<Object, OutMessage>>(
              DataFlowContext.sendPendingMax(cfg));
      pendingSendMessagesPerSource.put(s, pendingSendMessages);
      if (isKeyed) {
        serializerMap.put(s, new KeyedSerializer(new KryoSerializer(), workerId,
            keyType, dataType));
      } else {
        serializerMap.put(s, new AKeyedSerializer(new KryoSerializer(), workerId, dataType));
      }
    }

    int maxReceiveBuffers = DataFlowContext.receiveBufferCount(cfg);
    int receiveExecutorsSize = receivingExecutors().size();
    if (receiveExecutorsSize == 0) {
      receiveExecutorsSize = 1;
    }
    Set<Integer> execs = router.receivingExecutors();
    for (int e : execs) {
      int capacity = maxReceiveBuffers * 2 * receiveExecutorsSize;
      Queue<Pair<Object, InMessage>> pendingReceiveMessages =
          new ArrayBlockingQueue<>(
              capacity);
      pendingReceiveMessagesPerSource.put(e, pendingReceiveMessages);
      pendingReceiveDeSerializations.put(e, new ArrayBlockingQueue<>(capacity));
      if (isKeyed) {
        deSerializerMap.put(e, new KeyedDeSerializer(new KryoSerializer(),
            workerId, keyType, dataType));
      } else {
        deSerializerMap.put(e, new AKeyedDeserializer(workerId, dataType));
      }
    }

    Set<Integer> sourcesOfThisExec = TaskPlanUtils.getTasksOfThisWorker(taskPlan, sources);
    for (int s : sourcesOfThisExec) {
      sendRoutingParameters(s, pathToUse);
      partialSendRoutingParameters(s, pathToUse);
    }

    delegete.init(cfg, t, t, keyType, keyType, taskPlan, edge,
        router.receivingExecutors(), this,
        pendingSendMessagesPerSource, pendingReceiveMessagesPerSource,
        pendingReceiveDeSerializations, serializerMap, deSerializerMap, isKeyed);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    return delegete.sendMessagePartial(source, message, pathToUse, flags,
        partialSendRoutingParameters(source, pathToUse));
  }

  protected Set<Integer> receivingExecutors() {
    return router.receivingExecutors();
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return OperationUtils.getIntegerListMap(router, instancePlan, destination);
  }

  @Override
  public boolean isDelegateComplete() {
    return delegete.isComplete();
  }

  @Override
  public boolean isComplete() {
    boolean done = delegete.isComplete();
    boolean needsFurtherProgress = OperationUtils.progressReceivers(delegete, lock, finalReceiver,
        partialLock, partialReceiver);
    return done && !needsFurtherProgress;
  }

  @Override
  public boolean progress() {
    return OperationUtils.progressReceivers(delegete, lock,
        finalReceiver, partialLock, partialReceiver);
  }

  @Override
  public void close() {
    if (finalReceiver != null) {
      finalReceiver.close();
    }

    if (partialReceiver != null) {
      partialReceiver.close();
    }

    delegete.close();
  }

  @Override
  public void clean() {
    if (partialReceiver != null) {
      partialReceiver.clean();
    }

    if (finalReceiver != null) {
      finalReceiver.clean();
    }
  }

  @Override
  public void finish(int source) {
    while (!send(source, new byte[0], MessageFlags.SYNC_EMPTY)) {
      // lets progress until finish
      progress();
    }
  }

  @Override
  public MessageType getKeyType() {
    return keyType;
  }

  @Override
  public MessageType getDataType() {
    return dataType;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return instancePlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(edgeValue);
  }
}
