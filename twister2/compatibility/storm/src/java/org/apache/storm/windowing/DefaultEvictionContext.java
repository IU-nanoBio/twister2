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

package org.apache.storm.windowing;

public class DefaultEvictionContext implements EvictionContext {
  private final Long referenceTime;
  private final Long currentCount;
  private final Long slidingCount;
  private final Long slidingInterval;

  public DefaultEvictionContext(Long referenceTime) {
    this(referenceTime, null);
  }

  public DefaultEvictionContext(Long referenceTime, Long currentCount) {
    this(referenceTime, currentCount, null);
  }

  public DefaultEvictionContext(Long referenceTime, Long currentCount, Long slidingCount) {
    this(referenceTime, currentCount, slidingCount, null);
  }

  public DefaultEvictionContext(Long referenceTime, Long currentCount,
                                Long slidingCount, Long slidingInterval) {
    this.referenceTime = referenceTime;
    this.currentCount = currentCount;
    this.slidingCount = slidingCount;
    this.slidingInterval = slidingInterval;
  }

  @Override
  public Long getReferenceTime() {
    return referenceTime;
  }

  @Override
  public Long getCurrentCount() {
    return currentCount;
  }

  @Override
  public Long getSlidingCount() {
    return slidingCount;
  }

  @Override
  public Long getSlidingInterval() {
    return slidingInterval;
  }
}
