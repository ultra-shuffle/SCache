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

package org.scache.network.util;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

/**
 * A minimal base class for {@link FileRegion} implementations that want Netty's reference counting
 * behavior, and provides a default implementation for the legacy {@code transfered()} method.
 */
public abstract class AbstractFileRegion extends AbstractReferenceCounted implements FileRegion {

  @Override
  public FileRegion retain(int increment) {
    super.retain(increment);
    return this;
  }

  @Override
  public FileRegion retain() {
    super.retain();
    return this;
  }

  @Override
  public FileRegion touch(Object hint) {
    return this;
  }

  @Override
  public FileRegion touch() {
    super.touch();
    return this;
  }

  @Deprecated
  @Override
  public long transfered() {
    return transferred();
  }
}
