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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.MessageDirection;

public class AKeyedDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(AKeyedDeserializer.class.getName());

  private DataPacker dataPacker;

  public AKeyedDeserializer(int exec, MessageType dataType) {
    dataPacker = dataType.getDataPacker();
    LOG.fine("Initializing serializer on worker: " + exec);
  }

  @Override
  public void init(Config cfg, boolean k) {
  }

  /**
   * Builds the message from the data buffers in the partialObject. Since this method
   * supports multi-messages it iterates through the buffers and builds all the messages separately
   *
   * @param partialObject message object that needs to be built
   * @param edge the edge value associated with this message
   * @return the built message as a list of objects
   */
  @Override
  public Object build(Object partialObject, int edge) {
    InMessage currentMessage = (InMessage) partialObject;
    Queue<DataBuffer> buffers = currentMessage.getBuffers();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }

    List<DataBuffer> builtBuffers = new ArrayList<>();
    // get the number of objects deserialized
    DataBuffer buffer = buffers.peek();
    while (buffer != null) {
      int currentLocation = 0;
      int remaining = buffer.getSize();

      if (header.getNumberTuples() == 0) {
        builtBuffers.add(buffer);
        break;
      }

      // if we are at the beginning
      int currentObjectLength = currentMessage.getUnPkCurrentObjectLength();

      if (currentMessage.getUnPkBuffers() == 0) {
        currentLocation = 16;
        remaining = remaining - 16;
      } else {
        currentLocation = 4;
        remaining = remaining - 4;
      }

      if (currentObjectLength == -1 || currentMessage.getUnPkBuffers() == 0) {
        currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
        remaining = remaining - Integer.BYTES;
        currentLocation += Integer.BYTES;

        // starting to build a new object
        currentMessage.getDataBuilder().init(dataPacker, currentObjectLength);
      }


      while (remaining > 0) {
        // read the values from the buffer
        ObjectBuilderImpl dataBuilder = currentMessage.getDataBuilder();
        int bytesRead = dataPacker.readDataFromBuffer(
            dataBuilder, currentLocation,
            buffer
        );
        dataBuilder.incrementCompletedSizeBy(bytesRead);

        currentLocation += bytesRead;
        remaining = remaining - bytesRead;
        // okay we are done with this object
        if (dataBuilder.isBuilt()) {
          currentMessage.addCurrentObject();
        } else {
          // lets break the inner while loop
          break;
        }

        // could have next object length?
        if (remaining >= Integer.BYTES) {
          currentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
          remaining = remaining - Integer.BYTES;
          currentLocation += Integer.BYTES;

          currentMessage.getDataBuilder().init(dataPacker, currentObjectLength);
        } else {
          // we have to break here as we cannot read further
          break;
        }
      }

      // lets remove this buffer
      buffers.poll();
      builtBuffers.add(buffer);
      // increment the unpacked buffers
      currentMessage.incrementUnPkBuffers();

      // lets check weather we have read everythong
      int readObjectNumber = currentMessage.getUnPkNumberObjects();
      // we need to get number of tuples and get abs because we are using -1 for single messages
      if (readObjectNumber == Math.abs(currentMessage.getHeader().getNumberTuples())) {
        break;
      }

      // lets move to next
      buffer = buffers.peek();
    }


    if (builtBuffers.size() > 0) {
      ChannelMessage channelMessage = new ChannelMessage(currentMessage.getOriginatingId(),
          currentMessage.getDataType(), MessageDirection.IN, currentMessage.getReleaseListener());
      channelMessage.addBuffers(builtBuffers);
      channelMessage.setHeader(currentMessage.getHeader());
      channelMessage.incrementRefCount();
      currentMessage.addBuiltMessage(channelMessage);
    }
    return null;
  }

  @Override
  public Object getDataBuffers(Object partialObject, int edge) {
    return null;
  }

  /**
   * Builds the header object from the data in the data buffer
   *
   * @param buffer data buffer that contains the message
   * @param edge the edge value associated with this message
   * @return the built message header object
   */
  @Override
  public MessageHeader buildHeader(DataBuffer buffer, int edge) {
    int sourceId = buffer.getByteBuffer().getInt(0);
    int flags = buffer.getByteBuffer().getInt(Integer.BYTES);
    int destId = buffer.getByteBuffer().getInt(Integer.BYTES * 2);
    int length = buffer.getByteBuffer().getInt(Integer.BYTES * 3);

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    headerBuilder.flags(flags);
    headerBuilder.destination(destId);
    return headerBuilder.build();
  }
}
