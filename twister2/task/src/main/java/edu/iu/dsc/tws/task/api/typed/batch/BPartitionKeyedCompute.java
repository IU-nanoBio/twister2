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
package edu.iu.dsc.tws.task.api.typed.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.typed.AbstractIterableDataCompute;

public abstract class BPartitionKeyedCompute<K, T>
    extends AbstractIterableDataCompute<Tuple<K, T>> {

  public abstract boolean keyedPartition(Iterator<Tuple<K, T>> content);

  public boolean execute(IMessage<Iterator<Tuple<K, T>>> content) {
    return this.keyedPartition(content.getContent());
  }
}
