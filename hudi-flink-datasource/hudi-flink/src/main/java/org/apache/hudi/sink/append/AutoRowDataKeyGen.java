/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.append;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.sink.bulk.RowDataKeyGen;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Key generator for {@link RowData} that use an auto key generator.
 */
public class AutoRowDataKeyGen extends RowDataKeyGen {
  private final int taskId;
  private final String instantTime;
  private int rowId;

  public AutoRowDataKeyGen(
      int taskId,
      String instantTime,
      String partitionFields,
      RowType rowType,
      boolean hiveStylePartitioning,
      boolean encodePartitionPath) {
    super(null, partitionFields, rowType, hiveStylePartitioning, encodePartitionPath, false, null);
    this.taskId = taskId;
    this.instantTime = instantTime;
  }

  @Override
  public String getRecordKey(RowData rowData) {
    return HoodieRecord.generateSequenceId(instantTime, taskId, rowId++);
  }
}
