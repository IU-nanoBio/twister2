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
package edu.iu.dsc.tws.examples.ml.svm.streamer;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.examples.ml.svm.constant.Constants;
import edu.iu.dsc.tws.examples.ml.svm.util.DataGenerator;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class DataStreamer extends BaseSource {

  private static final Logger LOG = Logger.getLogger(DataStreamer.class.getName());

  private final int features = 10;

  private OperationMode operationMode;

  public DataStreamer(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public void execute() {

    double[] sendData = DataGenerator.seedDoubleArray(features);
    if (this.operationMode.equals(OperationMode.STREAMING)) {
      // do streaming sending
    }

    if (this.operationMode.equals(OperationMode.BATCH)) {
      this.context.write(Constants.SimpleGraphConfig.DATA_EDGE, sendData);
      this.context.end(Constants.SimpleGraphConfig.DATA_EDGE);
    }

  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }
}
