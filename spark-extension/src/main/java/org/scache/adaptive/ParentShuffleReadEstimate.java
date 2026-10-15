/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scache.adaptive;

import java.io.Serializable;

/** Immutable estimate of the direct parent shuffle inputs consumed by one stage. */
public final class ParentShuffleReadEstimate implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final ParentShuffleReadEstimate EMPTY =
      new ParentShuffleReadEstimate(0L, 0L, 0L, 0);

  private final long nativeBytes;
  private final long pbrBytes;
  private final long recordPbrBytes;
  private final int parentShuffleCount;

  public ParentShuffleReadEstimate(
      long nativeBytes,
      long pbrBytes,
      long recordPbrBytes,
      int parentShuffleCount) {
    this.nativeBytes = Math.max(0L, nativeBytes);
    this.pbrBytes = Math.max(0L, pbrBytes);
    this.recordPbrBytes = Math.max(0L, recordPbrBytes);
    this.parentShuffleCount = Math.max(0, parentShuffleCount);
  }

  public long nativeBytes() { return nativeBytes; }
  public long pbrBytes() { return pbrBytes; }
  public long recordPbrBytes() { return recordPbrBytes; }
  public int parentShuffleCount() { return parentShuffleCount; }

  public long totalBytes() {
    return saturatedAdd(nativeBytes, saturatedAdd(pbrBytes, recordPbrBytes));
  }

  public long cxlBytes() {
    return saturatedAdd(pbrBytes, recordPbrBytes);
  }

  private static long saturatedAdd(long left, long right) {
    if (left >= Long.MAX_VALUE - right) return Long.MAX_VALUE;
    return left + right;
  }
}
