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

#pragma once

#include "checkpoint_types.h"
#include "cursor.h"
#include "ep_types.h"
#include "monotonic.h"
#include "queue_op.h"

#include <boost/optional.hpp>
#include <memcached/engine_common.h>
#include <memcached/vbucket.h>
#include <memory>
#include <unordered_map>
#include <utility>

class Checkpoint;
class CheckpointConfig;
class CheckpointCursor;
class EPStats;
class PreLinkDocumentContext;
class VBucket;

template <typename... RV>
class Callback;

using LockHolder = std::lock_guard<std::mutex>;

/**
 * snapshot_range_t + a HCS for flushing to disk from Disk checkpoints which
 * is required as we can't work out a correct PCS on a replica due to de-dupe.
 */
struct CheckpointSnapshotRange {
    // Getters for start and end to allow us to use this in the same way as a
    // normal snapshot_range_t
    uint64_t getStart() {
        return range.getStart();
    }
    uint64_t getEnd() {
        return range.getEnd();
    }

    snapshot_range_t range;

    // HCS that should be flushed. Currently should only be set for Disk
    // Checkpoint runs.
    boost::optional<uint64_t> highCompletedSeqno = {};
};

/**
 * Representation of a checkpoint manager that maintains the list of checkpoints
 * for a given vbucket.
 */
class CheckpointManager {
    friend class Checkpoint;
    friend class EventuallyPersistentEngine;
    friend class Consumer;
    friend class CheckpointManagerTestIntrospector;

public:
    typedef std::shared_ptr<Callback<Vbid>> FlusherCallback;

    /// Return type of getNextItemsForCursor()
    struct ItemsForCursor {
        ItemsForCursor() {
        }
        ItemsForCursor(CheckpointType checkpointType,
                       boost::optional<uint64_t> maxDeletedRevSeqno)
            : checkpointType(checkpointType),
              maxDeletedRevSeqno(maxDeletedRevSeqno) {
        }
        std::vector<CheckpointSnapshotRange> ranges;
        bool moreAvailable = {false};
        CheckpointType checkpointType = CheckpointType::Memory;

        boost::optional<uint64_t> maxDeletedRevSeqno = {};
    };

    CheckpointManager(EPStats& st,
                      Vbid vbucket,
                      CheckpointConfig& config,
                      int64_t lastSeqno,
                      uint64_t lastSnapStart,
                      uint64_t lastSnapEnd,
                      FlusherCallback cb);

    uint64_t getOpenCheckpointId();

    uint64_t getLastClosedCheckpointId();

    void setOpenCheckpointId(uint64_t id);

    /**
     * Remove closed unreferenced checkpoints and return them through the
     * vector.
     * @param vbucket the vbucket that this checkpoint manager belongs to.
     * @param newOpenCheckpointCreated the flag indicating if the new open
     * checkpoint was created as a result of running this function.
     * @return the number of items that are purged from checkpoint
     * @param limit Max number of checkpoint that can be removed.
     *     No limit by default, overidden only for testing.
     */
    size_t removeClosedUnrefCheckpoints(
            VBucket& vbucket,
            bool& newOpenCheckpointCreated,
            size_t limit = std::numeric_limits<size_t>::max());

    /**
     * Attempt to expel (i.e. eject from memory) items in the oldest checkpoint
     * that still has cursor registered in it.  This is to help avoid very large
     * checkpoints which consume a large amount of memory.
     * @returns  ExpelResult - this is a structure containing two elements.  The
     * first element is the number of that have been expelled. The second
     * element is an estimate of the amount of memory that will be recovered.
     */
    ExpelResult expelUnreferencedCheckpointItems();

    /**
     * Register the cursor for getting items whose bySeqno values are between
     * startBySeqno and endBySeqno, and close the open checkpoint if endBySeqno
     * belongs to the open checkpoint.
     * @param startBySeqno start bySeqno.
     * @return Cursor registration result which consists of (1) the bySeqno with
     * which the cursor can start and (2) flag indicating if the cursor starts
     * with the first item on a checkpoint.
     */
    CursorRegResult registerCursorBySeqno(const std::string& name,
                                          uint64_t startBySeqno);

    /**
     * Remove the cursor for a given connection.
     * @param const pointer to the clients cursor, can be null
     * @return true if the cursor is removed successfully.
     */
    bool removeCursor(const CheckpointCursor* cursor);

    /**
     * Queue an item to be written to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     * @param qi item to be persisted.
     * @param generateBySeqno yes/no generate the seqno for the item
     * @param preLinkDocumentContext A context object needed for the
     *        pre link document API in the server API. It is notified
     *        with the generated CAS before the object is made available
     *        for other threads. May be nullptr if the document originates
     *        from a context where the document shouldn't be updated.
     * @return true if an item queued increases the size of persistence queue by 1.
     */
    bool queueDirty(VBucket& vb,
                    queued_item& qi,
                    const GenerateBySeqno generateBySeqno,
                    const GenerateCas generateCas,
                    PreLinkDocumentContext* preLinkDocumentContext);

    /*
     * Queue writing of the VBucket's state to persistent layer.
     * @param vb the vbucket that a new item is pushed into.
     */
    void queueSetVBState(VBucket& vb);

    /**
     * Add all outstanding items for the given cursor name to the vector. Only
     * fetches items for contiguous Checkpoints of the same type.
     *
     * @param cursor CheckpointCursor to read items from and advance
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    CheckpointManager::ItemsForCursor getNextItemsForCursor(
            CheckpointCursor* cursor, std::vector<queued_item>& items);

    /**
     * Add all outstanding items for persistence to the vector. Only fetches
     * items for contiguous Checkpoints of the same type.
     *
     * @param items container which items will be appended to.
     * @return The low/high sequence number added to `items` on success,
     *         or (0,0) if no items were added.
     */
    CheckpointManager::ItemsForCursor getNextItemsForPersistence(
            std::vector<queued_item>& items) {
        return getNextItemsForCursor(persistenceCursor, items);
    }

    /**
     * Add items for the given cursor to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The cursor is
     * advanced to point after the items fetched. Only fetches items for
     * contiguous Checkpoints of the same type.
     *
     * Note: It is only valid to fetch complete checkpoints; as such we cannot
     * limit to a precise number of items.
     *
     * @param cursor CheckpointCursor to read items from and advance
     * @param[in/out] items container which items will be appended to.
     * @param approxLimit Approximate number of items to add.
     * @return An ItemsForCursor object containing:
     * range: the low/high sequence number of the checkpoints(s) added to
     * `items`;
     * moreAvailable: true if there are still items available for this
     * checkpoint (i.e. the limit was hit).
     */
    ItemsForCursor getItemsForCursor(CheckpointCursor* cursor,
                                     std::vector<queued_item>& items,
                                     size_t approxLimit);

    /**
     * Add items for persistence to the vector, stopping on a checkpoint
     * boundary which is greater or equal to `approxLimit`. The persistence
     * cursor is advanced to point after the items fetched. Only fetches
     * items for contiguous Checkpoints of the same type.
     *
     * Note: It is only valid to fetch complete checkpoints; as such we cannot
     * limit to a precise number of items.
     *
     * @param[in/out] items container which items will be appended to.
     * @param approxLimit Approximate number of items to add.
     * @return An ItemsForCursor object containing:
     * range: the low/high sequence number of the checkpoints(s) added to
     * `items`;
     * moreAvailable: true if there are still items available for this
     * checkpoint (i.e. the limit was hit).
     */
    ItemsForCursor getItemsForPersistence(std::vector<queued_item>& items,
                                          size_t approxLimit) {
        return getItemsForCursor(persistenceCursor, items, approxLimit);
    }

    /**
     * Return the total number of items (including meta items) that belong to
     * this checkpoint manager.
     */
    size_t getNumItems() const {
        return numItems;
    }

    /**
     * Returns the number of non-meta items in the currently open checkpoint.
     */
    size_t getNumOpenChkItems() const;

    /* WARNING! This method can return inaccurate counts - see MB-28431. It
     * at *least* can suffer from overcounting by at least 1 (in scenarios as
     * yet not clear).
     * As such it is *not* safe to use when a precise count of remaining
     * items is needed.
     *
     * Returns the count of Items (excluding meta items) that the given cursor
     * has yet to process (i.e. between the cursor's current position and the
     * end of the last checkpoint).
     */
    size_t getNumItemsForCursor(const CheckpointCursor* cursor) const;

    /* WARNING! This method can return inaccurate counts - see MB-28431. It
     * at *least* can suffer from overcounting by at least 1 (in scenarios as
     * yet not clear).
     * As such it is *not* safe to use when a precise count of remaining
     * items is needed.
     *
     * Returns the count of Items (excluding meta items) that the persistence
     * cursor has yet to process (i.e. between the cursor's current position and
     * the end of the last checkpoint).
     */
    size_t getNumItemsForPersistence() const {
        return getNumItemsForCursor(persistenceCursor);
    }

    void clear(vbucket_state_t vbState);

    /**
     * Clear all the checkpoints managed by this checkpoint manager.
     */
    void clear(VBucket& vb, uint64_t seqno);

    const CheckpointConfig &getCheckpointConfig() const {
        return checkpointConfig;
    }

    void addStats(const AddStatFn& add_stat, const void* cookie);

    /**
     * Create a new open checkpoint by force.
     * @return the new open checkpoint id
     */
    uint64_t createNewCheckpoint();

    /**
     * Get id of the previous checkpoint that is followed by the checkpoint
     * where the persistence cursor is currently walking.
     */
    uint64_t getPersistenceCursorPreChkId();

    /**
     * Update the checkpoint manager persistence cursor checkpoint offset
     */
    void itemsPersisted();

    /**
     * Return memory consumption of all the checkpoints managed
     */
    size_t getMemoryUsage_UNLOCKED() const;

    size_t getMemoryUsage() const;

    /**
     * Return memory overhead of all the checkpoints managed
     */
    size_t getMemoryOverhead_UNLOCKED() const;

    /**
     * Return memory overhead of all the checkpoints managed
     */
    size_t getMemoryOverhead() const;

    /**
     * Return memory consumption of unreferenced checkpoints
     */
    size_t getMemoryUsageOfUnrefCheckpoints() const;

    /**
     * Function returns a list of cursors to drop so as to unreference
     * certain checkpoints within the manager, invoked by the cursor-dropper.
     * @return a container of weak_ptr to cursors
     */
    std::vector<Cursor> getListOfCursorsToDrop();

    /**
     * @return True if at least one checkpoint is unreferenced and can
     * be removed.
     */
    bool hasClosedCheckpointWhichCanBeRemoved() const;

    void setBackfillPhase(uint64_t start, uint64_t end);

    void createSnapshot(uint64_t snapStartSeqno,
                        uint64_t snapEndSeqno,
                        boost::optional<uint64_t> highCompletedSeqno,
                        CheckpointType checkpointType);

    void resetSnapshotRange();

    void updateCurrentSnapshot(uint64_t snapEnd, CheckpointType checkpointType);

    snapshot_info_t getSnapshotInfo();

    uint64_t getOpenSnapshotStartSeqno() const;

    bool incrCursor(CheckpointCursor &cursor);

    void notifyFlusher();

    void setBySeqno(int64_t seqno);

    int64_t getHighSeqno() const;

    int64_t nextBySeqno();

    /// @return the persistence cursor which can be null
    CheckpointCursor* getPersistenceCursor() const {
        return persistenceCursor;
    }

    void dump() const;

    /**
     * Take the cursors from another checkpoint manager and reset them in the
     * process - used as part of vbucket reset.
     * @param other the manager we are taking cursors from
     */
    void takeAndResetCursors(CheckpointManager& other);

    /// @return true if the current open checkpoint is a DiskCheckpoint
    bool isOpenCheckpointDisk();

protected:
    uint64_t getOpenCheckpointId_UNLOCKED(const LockHolder& lh);

    uint64_t getLastClosedCheckpointId_UNLOCKED(const LockHolder& lh);

    void setOpenCheckpointId_UNLOCKED(const LockHolder& lh, uint64_t id);

    // Helper method for queueing methods - update the global and per-VBucket
    // stats after queueing a new item to a checkpoint.
    // Must be called with queueLock held (LockHolder passed in as argument to
    // 'prove' this).
    void updateStatsForNewQueuedItem_UNLOCKED(const LockHolder& lh,
                                              VBucket& vb,
                                              const queued_item& qi);

    CheckpointList checkpointList;

    // Pair of {sequence number, cursor at checkpoint start} used when
    // updating cursor positions when collapsing checkpoints.
    struct CursorPosition {
        uint64_t seqno;
        bool onCpktStart;
    };

    bool removeCursor_UNLOCKED(const CheckpointCursor* cursor);

    CursorRegResult registerCursorBySeqno_UNLOCKED(const LockHolder& lh,
                                                   const std::string& name,
                                                   uint64_t startBySeqno);

    size_t getNumItemsForCursor_UNLOCKED(const CheckpointCursor* cursor) const;

    void clear_UNLOCKED(vbucket_state_t vbState, uint64_t seqno);

    /*
     * @return a reference to the open checkpoint
     */
    Checkpoint& getOpenCheckpoint_UNLOCKED(const LockHolder& lh) const;

    /*
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     *
     * @param id for the new checkpoint
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     * @param highCompletedSeqno optional SyncRep HCS to be flushed to disk
     * @param checkpointType is the checkpoint created from a replica receiving
     *                       a disk snapshot?
     */
    void addNewCheckpoint_UNLOCKED(uint64_t id,
                                   uint64_t snapStartSeqno,
                                   uint64_t snapEndSeqno,
                                   boost::optional<uint64_t> highCompletedSeqno,
                                   CheckpointType checkpointType);

    /*
     * Closes the current open checkpoint and adds a new open checkpoint to
     * the checkpointList.
     * Note: the function sets snapStart and snapEnd to 'lastBySeqno' for the
     *     new checkpoint.
     *
     * @param id for the new checkpoint
     */
    void addNewCheckpoint_UNLOCKED(uint64_t id);

    /*
     * Add an open checkpoint to the checkpointList.
     *
     * @param id for the new checkpoint
     * @param snapStartSeqno for the new checkpoint
     * @param snapEndSeqno for the new checkpoint
     * @param highCompletedSeqno the SyncRepl HCS to be flushed to disk
     * @param checkpointType is the checkpoint created from a replica receiving
     *                       a disk snapshot?
     */
    void addOpenCheckpoint(uint64_t id,
                           uint64_t snapStart,
                           uint64_t snapEnd,
                           boost::optional<uint64_t> highCompletedSeqno,
                           CheckpointType checkpointType);

    bool moveCursorToNextCheckpoint(CheckpointCursor &cursor);

    /**
     * Check the current open checkpoint to see if we need to create the new open checkpoint.
     * @param forceCreation is to indicate if a new checkpoint is created due to online update or
     * high memory usage.
     * @param timeBound is to indicate if time bound should be considered in creating a new
     * checkpoint.
     * @return the previous open checkpoint Id if we create the new open checkpoint. Otherwise
     * return 0.
     */
    uint64_t checkOpenCheckpoint_UNLOCKED(const LockHolder& lh,
                                          bool forceCreation,
                                          bool timeBound);

    bool isLastMutationItemInCheckpoint(CheckpointCursor &cursor);

    bool isCheckpointCreationForHighMemUsage_UNLOCKED(const LockHolder& lh,
                                                      const VBucket& vbucket);

    void resetCursors(bool resetPersistenceCursor = true);

    queued_item createCheckpointItem(uint64_t id,
                                     Vbid vbid,
                                     queue_op checkpoint_op);

    EPStats                 &stats;
    CheckpointConfig        &checkpointConfig;
    mutable std::mutex       queueLock;
    const Vbid vbucketId;

    // Total number of items (including meta items) in /all/ checkpoints managed
    // by this object.
    std::atomic<size_t>      numItems;
    Monotonic<int64_t>       lastBySeqno;
    uint64_t                 pCursorPreCheckpointId;

    /**
     * connCursors: stores all known CheckpointCursor objects which are held via
     * shared_ptr. When a client creates a cursor we store the shared_ptr and
     * give out a weak_ptr allowing cursors to be simply de-registered. We use
     * the client's chosen name as the key
     */
    using cursor_index =
            std::unordered_map<std::string, std::shared_ptr<CheckpointCursor>>;
    cursor_index connCursors;

    const FlusherCallback flusherCB;

    static constexpr const char* pCursorName = "persistence";
    Cursor pCursor;
    CheckpointCursor* persistenceCursor = nullptr;

    friend std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
};

// Outputs a textual description of the CheckpointManager.
std::ostream& operator<<(std::ostream& os, const CheckpointManager& m);
