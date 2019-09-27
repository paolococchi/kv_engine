/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "paging_visitor.h"
#include "bucket_logger.h"
#include "checkpoint_manager.h"
#include "connmap.h"
#include "dcp/dcpconnmap.h"
#include "ep_engine.h"
#include "ep_time.h"
#include "executorpool.h"
#include "item.h"
#include "item_eviction.h"
#include "kv_bucket.h"
#include "kv_bucket_iface.h"

#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <string>
#include <utility>

#include <phosphor/phosphor.h>
#include <memory>

static const size_t MAX_PERSISTENCE_QUEUE_SIZE = 1000000;

PagingVisitor::PagingVisitor(KVBucket& s,
                             EPStats& st,
                             double pcnt,
                             std::shared_ptr<std::atomic<bool>>& sfin,
                             pager_type_t caller,
                             bool pause,
                             double bias,
                             const VBucketFilter& vbFilter,
                             std::atomic<item_pager_phase>* phase,
                             bool _isEphemeral,
                             size_t agePercentage,
                             size_t freqCounterAgeThreshold)
    : ejected(0),
      freqCounterThreshold(0),
      ageThreshold(0),
      store(s),
      stats(st),
      percent(pcnt),
      activeBias(bias),
      startTime(ep_real_time()),
      stateFinalizer(sfin),
      owner(caller),
      canPause(pause),
      isBelowLowWaterMark(false),
      wasHighMemoryUsage(s.isMemoryUsageTooHigh()),
      taskStart(std::chrono::steady_clock::now()),
      pager_phase(phase),
      isEphemeral(_isEphemeral),
      agePercentage(agePercentage),
      freqCounterAgeThreshold(freqCounterAgeThreshold),
      maxCas(0) {
    setVBucketFilter(vbFilter);
}

bool PagingVisitor::visit(const HashTable::HashBucketLock& lh, StoredValue& v) {
    // The ItemPager should never touch a prepare. Prepares will be eventually
    // purged, but should not expire, whether completed or pending.
    if (v.isPending() || v.isCompleted()) {
        return true;
    }

    // Delete expired items for an active vbucket.
    bool isExpired = (currentBucket->getState() == vbucket_state_active) &&
                     v.isExpired(startTime) && !v.isDeleted();
    if (isExpired || v.isTempNonExistentItem() || v.isTempDeletedItem()) {
        std::unique_ptr<Item> it = v.toItem(currentBucket->getId());
        expired.push_back(*it.get());
        return true;
    }

    // return if not ItemPager, which uses valid eviction percentage
    if (percent <= 0 || !pager_phase) {
        return true;
    }

    /*
     * We take a copy of the freqCounterValue because calling
     * doEviction can modify the value, and when we want to
     * add it to the histogram we want to use the original value.
     */
    auto storedValueFreqCounter = v.getFreqCounterValue();
    bool evicted = true;

    /*
     * Calculate the age when the item was last stored / modified.
     * We do this by taking the item's current cas from the maxCas
     * (which is the maximum cas value of the current vbucket just
     * before we begin visiting all the items in the hash table).
     *
     * The time is actually stored in the top 48 bits of the cas
     * therefore we shift the age by casBitsNotTime.
     *
     * Note: If the item was written before we switched over to the
     * hybrid logical clock (HLC) (i.e. the item was written when the
     * bucket was 4.0/3.x etc...) then the cas value will be low and
     * so the item will appear very old.  However, this does not
     * matter as it just means that is likely to be evicted.
     */
    uint64_t age = (maxCas > v.getCas()) ? (maxCas - v.getCas()) : 0;
    age = age >> ItemEviction::casBitsNotTime;

    if ((storedValueFreqCounter <= freqCounterThreshold) &&
        ((storedValueFreqCounter < freqCounterAgeThreshold) ||
         (age >= ageThreshold))) {
        /*
         * If the storedValue is eligible for eviction then add its
         * frequency counter value to the histogram, otherwise add the
         * maximum (255) to indicate that the storedValue cannot be
         * evicted.
         *
         * By adding the maximum value for each storedValue that cannot
         * be evicted we ensure that the histogram is biased correctly
         * so that we get a frequency threshold that will remove the
         * correct number of storedValue items.
         */
        if (!doEviction(lh, &v)) {
            evicted = false;
            storedValueFreqCounter = std::numeric_limits<uint8_t>::max();
        }
    } else {
        evicted = false;
        // If the storedValue is NOT eligible for eviction then
        // we want to add the maximum value (255).
        if (!currentBucket->eligibleToPageOut(lh, v)) {
            storedValueFreqCounter = std::numeric_limits<uint8_t>::max();
        } else {
            /*
             * MB-29333 - For items that we have visited and did not
             * evict just because their frequency counter was too high,
             * the frequency counter must be decayed by 1 to
             * ensure that they will get evicted if repeatedly
             * visited (and assuming their frequency counter is not
             * incremented in between visits of the item pager).
             */
            if (storedValueFreqCounter > 0) {
                v.setFreqCounterValue(storedValueFreqCounter - 1);
            }
        }
    }
    itemEviction.addFreqAndAgeToHistograms(storedValueFreqCounter, age);

    if (evicted) {
        /**
         * Note: We are not taking a reader lock on the vbucket state.
         * Therefore it is possible that the stats could be slightly
         * out.  However given that its just for stats we don't want
         * to incur any performance cost associated with taking the
         * lock.
         */
        auto& frequencyValuesEvictedHisto =
                ((currentBucket->getState() == vbucket_state_active) ||
                 (currentBucket->getState() == vbucket_state_pending))
                        ? stats.activeOrPendingFrequencyValuesEvictedHisto
                        : stats.replicaFrequencyValuesEvictedHisto;
        frequencyValuesEvictedHisto.addValue(storedValueFreqCounter);
    }

    // Whilst we are learning it is worth always updating the
    // threshold. We also want to update the threshold at periodic
    // intervals.
    if (itemEviction.isLearning() || itemEviction.isRequiredToUpdate()) {
        auto thresholds =
                itemEviction.getThresholds(percent * 100.0, agePercentage);
        freqCounterThreshold = thresholds.first;
        ageThreshold = thresholds.second;
    }

    return true;
}

void PagingVisitor::visitBucket(const VBucketPtr& vb) {
    update();
    removeClosedUnrefCheckpoints(*vb);

    // fast path for expiry item pager
    if (percent <= 0 || !pager_phase) {
        if (vBucketFilter(vb->getId())) {
            currentBucket = vb;
            // EvictionPolicy is not required when running expiry item
            // pager
            vb->ht.visit(*this);
        }
        return;
    }

    // skip active vbuckets if active resident ratio is lower than replica
    double current = static_cast<double>(stats.getEstimatedTotalMemoryUsed());
    double lower = static_cast<double>(stats.mem_low_wat);
    double high = static_cast<double>(stats.mem_high_wat);
    if (vb->getState() == vbucket_state_active && current < high &&
        store.getActiveResidentRatio() < store.getReplicaResidentRatio()) {
        return;
    }

    if (current > lower) {
        double p = (current - static_cast<double>(lower)) / current;
        adjustPercent(p, vb->getState());
        if (vBucketFilter(vb->getId())) {
            currentBucket = vb;
            maxCas = currentBucket->getMaxCas();
            itemEviction.reset();
            freqCounterThreshold = 0;

            // Percent of items in the hash table to be visited
            // between updating the interval.
            const double percentOfItems = 0.1;
            // Calculate the number of items to visit before updating
            // the interval
            uint64_t noOfItems =
                    std::ceil(vb->getNumItems() * (percentOfItems * 0.01));
            uint64_t interval = (noOfItems > ItemEviction::learningPopulation)
                                        ? noOfItems
                                        : ItemEviction::learningPopulation;
            itemEviction.setUpdateInterval(interval);

            vb->ht.visit(*this);
            /**
             * Note: We are not taking a reader lock on the vbucket state.
             * Therefore it is possible that the stats could be slightly
             * out.  However given that its just for stats we don't want
             * to incur any performance cost associated with taking the
             * lock.
             */
            const bool isActiveOrPending =
                    ((currentBucket->getState() == vbucket_state_active) ||
                     (currentBucket->getState() == vbucket_state_pending));

            // Take a snapshot of the latest frequency histogram
            if (isActiveOrPending) {
                stats.activeOrPendingFrequencyValuesSnapshotHisto.reset();
                itemEviction.copyFreqHistogram(
                        stats.activeOrPendingFrequencyValuesSnapshotHisto);
            } else {
                stats.replicaFrequencyValuesSnapshotHisto.reset();
                itemEviction.copyFreqHistogram(
                        stats.replicaFrequencyValuesSnapshotHisto);
            }

            // We have just evicted all eligible items from the hash table
            // so we now want to reclaim the memory being used to hold
            // closed and unreferenced checkpoints in the vbucket, before
            // potentially moving to the next vbucket.
            removeClosedUnrefCheckpoints(*vb);
        }

    } else { // stop eviction whenever memory usage is below low watermark
        isBelowLowWaterMark = true;
    }
}

void PagingVisitor::update() {
    store.deleteExpiredItems(expired, ExpireBy::Pager);

    if (numEjected() > 0) {
        EP_LOG_DEBUG("Paged out {} values", numEjected());
    }

    size_t num_expired = expired.size();
    if (num_expired > 0) {
        EP_LOG_DEBUG("Purged {} expired items", num_expired);
    }

    ejected = 0;
    expired.clear();
}

bool PagingVisitor::pauseVisitor() {
    size_t queueSize = stats.diskQueueSize.load();
    return canPause && queueSize >= MAX_PERSISTENCE_QUEUE_SIZE;
}

void PagingVisitor::complete() {
    update();

    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - taskStart);
    if (owner == ITEM_PAGER) {
        stats.itemPagerHisto.add(elapsed_time);
    } else if (owner == EXPIRY_PAGER) {
        stats.expiryPagerHisto.add(elapsed_time);
    }

    bool inverse = false;
    (*stateFinalizer).compare_exchange_strong(inverse, true);

    if (pager_phase && !isBelowLowWaterMark) {
        if (*pager_phase == REPLICA_ONLY) {
            *pager_phase = ACTIVE_AND_PENDING_ONLY;
        } else if (*pager_phase == ACTIVE_AND_PENDING_ONLY && !isEphemeral) {
            *pager_phase = REPLICA_ONLY;
        }
    }

    // Wake up any sleeping backfill tasks if the memory usage is lowered
    // below the high watermark as a result of checkpoint removal.
    if (wasHighMemoryUsage && !store.isMemoryUsageTooHigh()) {
        store.notifyBackfillTasks();
    }

    if (ITEM_PAGER == owner) {
        // Re-check memory which may wake up the ItemPager and schedule
        // a new PagingVisitor with the next phase/memory target etc...
        // This is done after we've signalled 'completion' by clearing
        // the stateFinalizer, which ensures the ItemPager doesn't just
        // ignore a request.
        store.checkAndMaybeFreeMemory();
    }
}

// Removes checkpoints that are both closed and unreferenced, thereby
// freeing the associated memory.
// @param vb  The vbucket whose eligible checkpoints are removed from.
void PagingVisitor::removeClosedUnrefCheckpoints(VBucket& vb) {
    bool newCheckpointCreated = false;
    size_t removed = vb.checkpointManager->removeClosedUnrefCheckpoints(
            vb, newCheckpointCreated);
    stats.itemsRemovedFromCheckpoints.fetch_add(removed);
    // If the new checkpoint is created, notify this event to the
    // corresponding paused DCP connections.
    if (newCheckpointCreated) {
        store.getEPEngine().getDcpConnMap().notifyVBConnections(
                vb.getId(), vb.checkpointManager->getHighSeqno());
    }
}

void PagingVisitor::adjustPercent(double prob, vbucket_state_t state) {
    if (state == vbucket_state_replica || state == vbucket_state_dead) {
        // replica items should have higher eviction probability
        double p = prob * (2 - activeBias);
        percent = p < 0.9 ? p : 0.9;
    } else {
        // active items have lower eviction probability
        percent = prob * activeBias;
    }
}

bool PagingVisitor::doEviction(const HashTable::HashBucketLock& lh,
                               StoredValue* v) {
    auto policy = store.getItemEvictionPolicy();
    StoredDocKey key(v->getKey());

    if (currentBucket->pageOut(readHandle, lh, v)) {
        ++ejected;

        /**
         * For FULL EVICTION MODE, add all items that are being
         * evicted to the corresponding bloomfilter.
         */
        if (policy == ::EvictionPolicy::Full) {
            currentBucket->addToFilter(key);
        }
        // performed eviction so return true
        return true;
    }
    // did not perform eviction so return false
    return false;
}

void PagingVisitor::setUpHashBucketVisit() {
    // Grab a locked ReadHandle
    readHandle = currentBucket->lockCollections();
}

void PagingVisitor::tearDownHashBucketVisit() {
    // Unlock the readHandle. It can now never be locked again, and should
    // not be used until overwriting with a locked ReadHandle.
    readHandle.unlock();
}
