# Mesos Network Configuration



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.dfw.tcp.TWSTCPChannel"</td><tr><td>description</td><td></td></table>

**network.buffer.size**
<table><tr><td>default</td><td>1024000</td><tr><td>description</td><td>the buffer size to be used</td></table>

**network.sendBuffer.count**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>number of send buffers to be used</td></table>

**network.receiveBuffer.count**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>number of receive buffers to be used</td></table>

**network.channel.pending.size**
<table><tr><td>default</td><td>2048</td><tr><td>description</td><td>channel pending messages</td></table>

**network.send.pending.max**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td>the send pending messages</td></table>

**network.partition.message.group.low_water_mark**
<table><tr><td>default</td><td>8</td><tr><td>description</td><td>group up to 8 ~ 16 messages</td></table>

**network.partition.message.group.high_water_mark**
<table><tr><td>default</td><td>16</td><tr><td>description</td><td>this is the max number of messages to group</td></table>

**shuffle.memory.bytes.max**
<table><tr><td>default</td><td>1024000</td><tr><td>description</td><td>the maximum amount of bytes kept in memory for operations that goes to disk</td></table>

**shuffle.memory.records.max**
<table><tr><td>default</td><td>10240</td><tr><td>description</td><td>the maximum number of records kept in memory for operations that goes to dist</td></table>

**the keyed reduce algorithm**
<table><tr><td>default</td><td>partition</td><tr><td>description</td><td></td></table>

**the algorithm for gathering data**
<table><tr><td>default</td><td>partition</td><tr><td>description</td><td></td></table>

**the partitioning algorithm**
<table><tr><td>default</td><td>simple</td><tr><td>description</td><td></td></table>

**the keyed reduce algorithm**
<table><tr><td>default</td><td>partition</td><tr><td>description</td><td></td></table>

**the algorithm for gathering data**
<table><tr><td>default</td><td>partition</td><tr><td>description</td><td></td></table>

**the partitioning algorithm**
<table><tr><td>default</td><td>simple</td><tr><td>description</td><td></td></table>
