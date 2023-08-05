/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataV2MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanMigrator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;

/**
 * Utils for clean action.
 */
public class CleanerUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CleanerUtils.class);

  public static final Integer CLEAN_METADATA_VERSION_1 = CleanMetadataV1MigrationHandler.VERSION;
  public static final Integer CLEAN_METADATA_VERSION_2 = CleanMetadataV2MigrationHandler.VERSION;
  public static final Integer LATEST_CLEAN_METADATA_VERSION = CLEAN_METADATA_VERSION_2;

  public static HoodieCleanMetadata convertCleanMetadata(String startCleanTime,
                                                         Option<Long> durationInMs,
                                                         List<HoodieCleanStat> cleanStats) {
    Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = new HashMap<>();
    Map<String, HoodieCleanPartitionMetadata> partitionBootstrapMetadataMap = new HashMap<>();

    int totalDeleted = 0;
    String earliestCommitToRetain = null;
    String lastCompletedCommitTimestamp = "";
    for (HoodieCleanStat stat : cleanStats) {
      HoodieCleanPartitionMetadata metadata =
          new HoodieCleanPartitionMetadata(stat.getPartitionPath(), stat.getPolicy().name(),
              stat.getDeletePathPatterns(), stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles(), stat.isPartitionDeleted());
      partitionMetadataMap.put(stat.getPartitionPath(), metadata);
      if ((null != stat.getDeleteBootstrapBasePathPatterns())
          && (!stat.getDeleteBootstrapBasePathPatterns().isEmpty())) {
        HoodieCleanPartitionMetadata bootstrapMetadata = new HoodieCleanPartitionMetadata(stat.getPartitionPath(),
            stat.getPolicy().name(), stat.getDeleteBootstrapBasePathPatterns(), stat.getSuccessDeleteBootstrapBaseFiles(),
            stat.getFailedDeleteBootstrapBaseFiles(), stat.isPartitionDeleted());
        partitionBootstrapMetadataMap.put(stat.getPartitionPath(), bootstrapMetadata);
      }
      totalDeleted += stat.getSuccessDeleteFiles().size();
      if (earliestCommitToRetain == null) {
        // This will be the same for all partitions
        earliestCommitToRetain = stat.getEarliestCommitToRetain();
        lastCompletedCommitTimestamp = stat.getLastCompletedCommitTimestamp();
      }
    }

    return new HoodieCleanMetadata(startCleanTime, durationInMs.orElseGet(() -> -1L), totalDeleted, earliestCommitToRetain,
        lastCompletedCommitTimestamp, partitionMetadataMap, CLEAN_METADATA_VERSION_2, partitionBootstrapMetadataMap);
  }

  /**
   * Get Latest Version of Hoodie Cleaner Metadata - Output of cleaner operation.
   * @param metaClient Hoodie Table Meta Client
   * @param cleanInstant Instant referring to clean action
   * @return Latest version of Clean metadata corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanMetadata getCleanerMetadata(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant)
      throws IOException {
    CleanMetadataMigrator metadataMigrator = new CleanMetadataMigrator(metaClient);
    HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(
        metaClient.getActiveTimeline().readCleanerInfoAsBytes(cleanInstant).get());
    return metadataMigrator.upgradeToLatest(cleanMetadata, cleanMetadata.getVersion());
  }

  /**
   * Get Latest Version of Hoodie Cleaner Metadata - Output of cleaner operation.
   * @return Latest version of Clean metadata corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanMetadata getCleanerMetadata(HoodieTableMetaClient metaClient, byte[] details)
      throws IOException {
    CleanMetadataMigrator metadataMigrator = new CleanMetadataMigrator(metaClient);
    HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(details);
    return metadataMigrator.upgradeToLatest(cleanMetadata, cleanMetadata.getVersion());
  }

  public static Option<HoodieInstant> getEarliestCommitToRetain(
      HoodieTimeline commitsTimeline, HoodieCleaningPolicy cleaningPolicy, int commitsRetained,
      Instant latestInstant, int hoursRetained, HoodieTimelineTimeZone timeZone) {
    HoodieTimeline completedCommitsTimeline = commitsTimeline.filterCompletedInstants();
    Option<HoodieInstant> earliestCommitToRetain = Option.empty();

    if (cleaningPolicy == HoodieCleaningPolicy.KEEP_LATEST_COMMITS
        && completedCommitsTimeline.countInstants() > commitsRetained) {
      earliestCommitToRetain = completedCommitsTimeline.nthInstant(completedCommitsTimeline.countInstants()
          - commitsRetained); //15 instants total, 10 commits to retain, this gives 6th instant in the list
    } else if (cleaningPolicy == HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS) {
      ZonedDateTime latestDateTime = ZonedDateTime.ofInstant(latestInstant, timeZone.getZoneId());
      String earliestTimeToRetain = HoodieActiveTimeline.formatDate(Date.from(latestDateTime.minusHours(hoursRetained).toInstant()));
      earliestCommitToRetain = Option.fromJavaOptional(completedCommitsTimeline.getInstantsAsStream().filter(i -> HoodieTimeline.compareTimestamps(i.getTimestamp(),
          HoodieTimeline.GREATER_THAN_OR_EQUALS, earliestTimeToRetain)).findFirst());
    }
    return earliestCommitToRetain.map(earliestCommitToRetain0 -> maybeReplacedByEarliestPending(earliestCommitToRetain0, commitsTimeline));
  }

  /**
   * If there is a pending commit in timeline earlier than the earliestCommitToRetain generate by clean policy, use it as earliestCommitToRetain.
   */
  private static HoodieInstant maybeReplacedByEarliestPending(HoodieInstant earliestCommitToRetain, HoodieTimeline commitsTimeline) {
    Option<HoodieInstant> earliestPendingCommits =
        commitsTimeline.filter(s -> !s.isCompleted()).firstInstant();
    if (earliestPendingCommits.isPresent() && earliestCommitToRetain.getTimestamp().compareTo(earliestPendingCommits.get().getTimestamp()) > 0) {
      // Earliest commit to retain must not be later than the earliest pending commit
      earliestCommitToRetain = earliestPendingCommits.get();
    }
    return earliestCommitToRetain;
  }

  /**
   * Get Latest version of cleaner plan corresponding to a clean instant.
   *
   * @param metaClient   Hoodie Table Meta Client
   * @param cleanInstant Instant referring to clean action
   * @return Cleaner plan corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanerPlan getCleanerPlan(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant)
      throws IOException {
    CleanPlanMigrator cleanPlanMigrator = new CleanPlanMigrator(metaClient);
    HoodieCleanerPlan cleanerPlan = TimelineMetadataUtils.deserializeAvroMetadata(
        metaClient.getActiveTimeline().readCleanerInfoAsBytes(cleanInstant).get(), HoodieCleanerPlan.class);
    return cleanPlanMigrator.upgradeToLatest(cleanerPlan, cleanerPlan.getVersion());
  }

  /**
   * Get Latest version of cleaner plan corresponding to a clean instant.
   *
   * @param metaClient   Hoodie Table Meta Client
   * @return Cleaner plan corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanerPlan getCleanerPlan(HoodieTableMetaClient metaClient, byte[] details)
      throws IOException {
    CleanPlanMigrator cleanPlanMigrator = new CleanPlanMigrator(metaClient);
    HoodieCleanerPlan cleanerPlan = TimelineMetadataUtils.deserializeAvroMetadata(details, HoodieCleanerPlan.class);
    return cleanPlanMigrator.upgradeToLatest(cleanerPlan, cleanerPlan.getVersion());
  }

  /**
   * Convert list of cleanFileInfo instances to list of avro-generated HoodieCleanFileInfo instances.
   * @param cleanFileInfoList
   * @return
   */
  public static List<HoodieCleanFileInfo> convertToHoodieCleanFileInfoList(List<CleanFileInfo> cleanFileInfoList) {
    return cleanFileInfoList.stream().map(CleanFileInfo::toHoodieFileCleanInfo).collect(Collectors.toList());
  }

  /**
   * Execute {@link HoodieFailedWritesCleaningPolicy} to rollback failed writes for different actions.
   * @param cleaningPolicy
   * @param actionType
   * @param rollbackFailedWritesFunc
   */
  public static void rollbackFailedWrites(HoodieFailedWritesCleaningPolicy cleaningPolicy, String actionType,
                                          Functions.Function0<Boolean> rollbackFailedWritesFunc) {
    switch (actionType) {
      case HoodieTimeline.CLEAN_ACTION:
        if (cleaningPolicy.isEager()) {
          // No need to do any special cleanup for failed operations during clean
          return;
        } else if (cleaningPolicy.isLazy()) {
          LOG.info("Cleaned failed attempts if any");
          // Perform rollback of failed operations for all types of actions during clean
          rollbackFailedWritesFunc.apply();
          return;
        }
        // No action needed for cleaning policy NEVER
        break;
      case COMMIT_ACTION:
        // For any other actions, perform rollback of failed writes
        if (cleaningPolicy.isEager()) {
          LOG.info("Cleaned failed attempts if any");
          rollbackFailedWritesFunc.apply();
          return;
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported action type " + actionType);
    }
  }
}
