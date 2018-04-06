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
package edu.iu.dsc.tws.task.taskgraphfluentapi;

import edu.iu.dsc.tws.task.taskgraphbuilder.TaskEdge;

/**
 * The user has to define their task and taskgraph generation similar to this class.
 */
public class TaskGraphFluentAPITestCode {

  /**
   * This constructor is responsible for generating the task objects and the task graph...!
   */

  public TaskGraphFluentAPITestCode(int numberOfTasks) {


    ITaskInfo task1 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 1;
      }
    };

    ITaskInfo task2 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 2;
      }
    };

    ITaskInfo task3 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 3;
      }
    };

    ITaskInfo task4 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 4;
      }
    };

    new TaskGraph().taskgraphName("Task Graph Fluent API Testing")
        .show()
        .generateTaskVertex(0)
        .generateTaskVertex(1)
        .generateTaskVertex(2)
        .generateTaskVertex(3)

        .connectTaskVertex_Edge(0, 1, 2)
        .connectTaskVertex_Edge(1, 3)
        .connectTaskVertex_Edge(2, 3)

        .connectTaskVertex_Edge(task1)
        .connectTaskVertex_Edge(task1, task2, task3)
        .connectTaskVertex_Edge(task2, task4)
        .connectTaskVertex_Edge(task3, task4)

        //For testing
        .connectTaskVertex_Edge(new TaskEdge("Source"), task1)
        .connectTaskVertex_Edge(new TaskEdge("Map"), task1, task2)
        .connectTaskVertex_Edge(new TaskEdge("Reduce"), task1, task3)
        .connectTaskVertex_Edge(new TaskEdge("Aggregate1"), task2, task4)
        .connectTaskVertex_Edge(new TaskEdge("Aggregate2"), task3, task4)

        .build()
        .displayTaskGraph();
  }
}
