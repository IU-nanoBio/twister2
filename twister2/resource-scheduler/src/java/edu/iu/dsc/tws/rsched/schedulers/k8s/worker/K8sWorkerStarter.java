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
package edu.iu.dsc.tws.rsched.schedulers.k8s.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.resource.WorkerComputeSpec;
import edu.iu.dsc.tws.common.resource.ZResourcePlan;
import edu.iu.dsc.tws.common.util.ReflectionUtils;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.K8sEnvVariables;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.PodWatchUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;
import static edu.iu.dsc.tws.common.config.Context.JOB_ARCHIVE_DIRECTORY;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.KUBERNETES_CLUSTER_TYPE;
import static edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants.POD_MEMORY_VOLUME;

public final class K8sWorkerStarter {
  private static final Logger LOG = Logger.getLogger(K8sWorkerStarter.class.getName());

  private static Config config = null;
  private static int workerID = -1; // -1 means, not initialized
  private static WorkerNetworkInfo workerNetworkInfo;
  private static JobMasterClient jobMasterClient;
  private static String jobName = null;

  private K8sWorkerStarter() { }

  public static void main(String[] args) {
    // we can not initialize the logger fully yet,
    // but we need to set the format as the first thing
    LoggingHelper.setLoggingFormat(LoggingHelper.DEFAULT_FORMAT);

    // all environment variables
    int workerPort = Integer.parseInt(System.getenv(K8sEnvVariables.WORKER_PORT + ""));
    String containerName = System.getenv(K8sEnvVariables.CONTAINER_NAME + "");
    String jobMasterIP = System.getenv(K8sEnvVariables.JOB_MASTER_IP + "");
    String encodedNodeInfoList = System.getenv(K8sEnvVariables.ENCODED_NODE_INFO_LIST + "");
    jobName = System.getenv(K8sEnvVariables.JOB_NAME + "");
    if (jobName == null) {
      throw new RuntimeException("JobName is null");
    }

    // load the configuration parameters from configuration directory
    String configDir = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/"
        + KUBERNETES_CLUSTER_TYPE;

    config = K8sWorkerUtils.loadConfig(configDir);

    // update config with jobMasterIP
    config = Config.newBuilder()
        .putAll(config)
        .put(JobMasterContext.JOB_MASTER_IP, jobMasterIP)
        .build();

    // get podName and podIP from localhost
    InetAddress localHost = null;
    try {
      localHost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Cannot get localHost.", e);
    }

    String podIP = localHost.getHostAddress();
    String podName = localHost.getHostName();

    String nodeIP = PodWatchUtils.getNodeIP(KubernetesContext.namespace(config), jobName, podName);
    NodeInfo thisNodeInfo = KubernetesContext.nodeLocationsFromConfig(config)
        ? KubernetesContext.getNodeInfo(config, nodeIP)
        : K8sWorkerUtils.getNodeInfoFromEncodedStr(encodedNodeInfoList, nodeIP);

    LOG.info("NodeInfo for this worker: " + thisNodeInfo);

    // set workerID
    int containersPerPod = KubernetesContext.workersPerPod(config);
    workerID = K8sWorkerUtils.calculateWorkerID(podName, containerName, containersPerPod);

    // set workerNetworkInfo
    workerNetworkInfo = new WorkerNetworkInfo(localHost, workerPort, workerID, thisNodeInfo);

    // initialize persistent volume
    K8sPersistentVolume pv = null;
    if (KubernetesContext.persistentVolumeRequested(config)) {
      // create persistent volume object
      String persistentJobDir = KubernetesConstants.PERSISTENT_VOLUME_MOUNT;
      pv = new K8sPersistentVolume(persistentJobDir, workerID);
    }

    // initialize persistent logging
    K8sWorkerUtils.initWorkerLogger(workerID, pv, config);

    // read job description file
    String jobDescFileName = SchedulerContext.createJobDescriptionFileName(jobName);
    jobDescFileName = POD_MEMORY_VOLUME + "/" + JOB_ARCHIVE_DIRECTORY + "/" + jobDescFileName;
    JobAPI.Job job = JobUtils.readJobFile(null, jobDescFileName);
    LOG.info("Job description file is loaded: " + jobDescFileName);

    // add any configuration from job file to the config object
    // if there are the same config parameters in both,
    // job file configurations will override
    config = JobUtils.overrideConfigs(job, config);
    config = JobUtils.updateConfigs(job, config);

    LOG.info("Worker information summary: \n"
        + "workerID: " + workerID + "\n"
        + "POD_IP: " + podIP + "\n"
        + "HOSTNAME(podname): " + podName + "\n"
        + "workerPort: " + workerPort + "\n"
    );

    // start JobMasterClient
    jobMasterClient = K8sWorkerUtils.startJobMasterClient(config, workerNetworkInfo);
    if (jobMasterClient == null) {
      return;
    }

    // we need to make sure that the worker starting message went through
    jobMasterClient.sendWorkerStartingMessage();

    // we will be running the Worker, send running message
    jobMasterClient.sendWorkerRunningMessage();

    // start the worker
    startWorker(jobMasterClient.getJMWorkerController(), pv);

    // close the worker
    closeWorker();
  }

  /**
   * start the Worker class specified in conf files
   */
  public static void startWorker(IWorkerController workerController,
                                 IPersistentVolume pv) {

    String workerClass = SchedulerContext.containerClass(config);
    IWorker worker;
    try {
      Object object = ReflectionUtils.newInstance(workerClass);
      worker = (IWorker) object;
      LOG.info("loaded worker class: " + workerClass);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.severe(String.format("failed to load the worker class %s", workerClass));
      throw new RuntimeException(e);
    }

    K8sVolatileVolume volatileVolume = null;
    if (SchedulerContext.volatileDiskRequested(config)) {
      volatileVolume =
          new K8sVolatileVolume(SchedulerContext.jobName(config), workerID);
    }

    ZResourcePlan resourcePlan = generateResourcePlan();

    worker.init(config, workerID, resourcePlan, workerController, pv, volatileVolume);
  }

  /**
   * A ZResourcePlan object is created
   * For each worker in the job, a WorkerComputeSpec is created and it is added to the ZResourcePlan
   * Each WorkerComputeSpec has the podName of its worker as a property
   * @return
   */
  public static ZResourcePlan generateResourcePlan() {
    int numberOfWorkers = Context.workerInstances(config);
    int workersPerPod = KubernetesContext.workersPerPod(config);
    int numberOfPods = numberOfWorkers / workersPerPod;

    ZResourcePlan plan = new ZResourcePlan(SchedulerContext.clusterType(config), workerID);
    int nextWorkerID = 0;

    for (int i = 0; i < numberOfPods; i++) {

      String podName = KubernetesUtils.podNameFromJobName(jobName, i);
      for (int j = 0; j < workersPerPod; j++) {
        WorkerComputeSpec workerComputeSpec = new WorkerComputeSpec(nextWorkerID);
        workerComputeSpec.addProperty(SchedulerContext.WORKER_NAME, podName);
        plan.addContainer(workerComputeSpec);
        nextWorkerID++;
      }
    }

    return plan;
  }

  /**
   * last method to call to close the worker
   */
  public static void closeWorker() {

    // send worker completed message to the Job Master and finish
    // Job master will delete the StatefulSet object
    jobMasterClient.sendWorkerCompletedMessage();
    jobMasterClient.close();
  }

}
