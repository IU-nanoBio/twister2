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
package edu.iu.dsc.tws.tsched.batch.roundrobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class RoundRobinBatchTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinBatchTaskScheduling.class.getName());

  private static int taskSchedulePlanId = 0;
  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config cfg;

  @Override
  public void initialize(Config cfg1) {
    this.cfg = cfg1;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);
  }

  /**
   * This method is responsible for handling the batch dataflow task graph.
   */
  public List<TaskSchedulePlan> scheduleBatch(
      DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();
    Map<Integer, List<InstanceId>> roundrobinContainerInstanceMap;

    //Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());
    List<Set<Vertex>> taskVertexList = parseVertexSet(taskVertexSet, dataFlowTaskGraph);

    LOG.info("Task Vertex Set List Size: %%%%" + taskVertexList.size());

    for (int i = 0; i < taskVertexList.size(); i++) {

      Set<Vertex> vertexSet = taskVertexList.get(i);

      LOG.info("%%%% Task Vertex Set Size: %%%%" + vertexSet.size());

      if (vertexSet.size() > 1) {
        roundrobinContainerInstanceMap = RoundRobinBatchScheduling.
            RoundRobinBatchSchedulingAlgo(vertexSet, workerPlan.getNumberOfWorkers());
      } else {
        Vertex vertex = vertexSet.iterator().next();
        roundrobinContainerInstanceMap = RoundRobinBatchScheduling.
            RoundRobinBatchSchedulingAlgo(vertex, workerPlan.getNumberOfWorkers());
      }

      Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap =
          instanceMapCalculation.getInstancesRamMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
          instanceMapCalculation.getInstancesDiskMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
          instanceMapCalculation.getInstancesCPUMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      for (int containerId : roundrobinContainerInstanceMap.keySet()) {

        Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
        Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
        Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

        List<InstanceId> taskInstanceIds = roundrobinContainerInstanceMap.get(containerId);
        Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (InstanceId id : taskInstanceIds) {

          double instanceRAMValue = instancesRamMap.get(containerId).get(id);
          double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
          double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

          Resource instanceResource = new Resource(instanceRAMValue,
              instanceDiskValue, instanceCPUValue);

          taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
              id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

          containerRAMValue += instanceRAMValue;
          containerDiskValue += instanceDiskValue;
          containerCpuValue += instanceDiskValue;
        }

        Worker worker = workerPlan.getWorker(containerId);
        Resource containerResource;

        if (worker != null && worker.getCpu() > 0
            && worker.getDisk() > 0 && worker.getRam() > 0) {
          containerResource = new Resource((double) worker.getRam(),
              (double) worker.getDisk(), (double) worker.getCpu());
          LOG.fine(String.format("Worker (if loop):" + containerId + "\tRam:"
              + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
              + "\tCpu:" + worker.getCpu()));
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
          LOG.fine(String.format("Worker (else loop):" + containerId
              + "\tRam:" + containerRAMValue     //write into a log file
              + "\tDisk:" + containerDiskValue
              + "\tCpu:" + containerCpuValue));
        }
        if (taskInstancePlanMap.values() != null) {
          TaskSchedulePlan.ContainerPlan taskContainerPlan =
              new TaskSchedulePlan.ContainerPlan(containerId,
                  new HashSet<>(taskInstancePlanMap.values()), containerResource);
          containerPlans.add(taskContainerPlan);
        }
      }
      taskSchedulePlanList.add(new TaskSchedulePlan(taskSchedulePlanId, containerPlans));
      taskSchedulePlanId++;
    }

    for (int j = 0; j < taskSchedulePlanList.size(); j++) {
      TaskSchedulePlan taskSchedulePlan = taskSchedulePlanList.get(j);
      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
          = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan = containerPlan.getTaskInstances();
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.info("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
              + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
        }
      }
    }
    return taskSchedulePlanList;
  }

  @SuppressWarnings("unchecked")
  private List<Set<Vertex>> parseVertexSet(
      Set<Vertex> taskVertexSet, DataFlowTaskGraph dataFlowTaskGraph) {

    List<Set<Vertex>> taskVertexList = new ArrayList<>();

    for (Vertex vertex : taskVertexSet) {
      if (dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() >= 2) {
        Set<Vertex> parentTask = new LinkedHashSet<>();
        parentTask.add(vertex);
        taskVertexList.add(parentTask);

        LinkedHashSet<Vertex> vertexSet = (LinkedHashSet) dataFlowTaskGraph.childrenOfTask(vertex);
        taskVertexList.add(vertexSet);

      } else if (dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() >= 2) {
        Set<Vertex> parentTask1 = new LinkedHashSet<>();
        for (int i = 0; i < taskVertexList.size(); i++) {
          Set<Vertex> vv = taskVertexList.get(i);
          for (Vertex vertex1 : vv) {
            if (!vertex1.getName().equals(vv) && !parentTask1.contains(vertex)) {
              LOG.info("vv details:" + vertex1.getName()
                  + "\tvertex details:" + vertex.getName());
              parentTask1.add(vertex);
              taskVertexList.add(parentTask1);
            }
          }
        }
      }
    }
    return taskVertexList;
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Map<Integer, List<InstanceId>> roundrobinContainerInstanceMap;

    for (Vertex vertex : taskVertexSet) {
      roundrobinContainerInstanceMap = RoundRobinBatchScheduling.
          RoundRobinBatchSchedulingAlgo(vertex, workerPlan.getNumberOfWorkers());

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap =
          instanceMapCalculation.getInstancesRamMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
          instanceMapCalculation.getInstancesDiskMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
          instanceMapCalculation.getInstancesCPUMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      for (int containerId : roundrobinContainerInstanceMap.keySet()) {

        Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
        Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
        Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

        List<InstanceId> taskInstanceIds = roundrobinContainerInstanceMap.get(containerId);
        Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (InstanceId id : taskInstanceIds) {
          double instanceRAMValue = instancesRamMap.get(containerId).get(id);
          double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
          double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

          LOG.info(String.format("Container Id:" + containerId
              + "Task Id and Index\t" + id.getTaskId() + "\t" + id.getTaskIndex()
              + "\tand Req. Resource:" + instanceRAMValue + "\t" + instanceDiskValue
              + "\t" + instanceCPUValue));

          Resource instanceResource = new Resource(instanceRAMValue,
              instanceDiskValue, instanceCPUValue);

          taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
              id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

          containerRAMValue += instanceRAMValue;
          containerDiskValue += instanceDiskValue;
          containerCpuValue += instanceDiskValue;
        }

        Worker worker = workerPlan.getWorker(containerId);
        Resource containerResource;

        if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
          containerResource = new Resource((double) worker.getRam(),
              (double) worker.getDisk(), (double) worker.getCpu());
          LOG.fine(String.format("Worker (if loop):" + containerId + "\tRam:"
              + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
              + "\tCpu:" + worker.getCpu()));
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
          LOG.fine(String.format("Worker (else loop):" + containerId
              + "\tRam:" + containerRAMValue     //write into a log file
              + "\tDisk:" + containerDiskValue
              + "\tCpu:" + containerCpuValue));
        }

        TaskSchedulePlan.ContainerPlan taskContainerPlan =
            new TaskSchedulePlan.ContainerPlan(containerId,
                new HashSet<>(taskInstancePlanMap.values()), containerResource);
        containerPlans.add(taskContainerPlan);
      }
    }
    //new TaskSchedulePlan(job.getJobId(), containerPlans);
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }

  @Override
  public TaskSchedulePlan tschedule() throws ScheduleException {
    return null;
  }

  @Override
  public void close() {
  }
}