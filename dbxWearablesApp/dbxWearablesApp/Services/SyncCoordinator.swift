import Foundation
import HealthKit
import OSLog

/// Orchestrates the sync cycle: query HealthKit → map to models → serialize NDJSON → POST → update anchor.
///
/// Each sample type is synced independently with its own query → POST → anchor-persist cycle.
/// This means each type makes independent progress within a background execution window (~30s).
/// If time runs out after syncing 6 of 14 types, those 6 are done — the next wake picks up
/// the remaining 8 from their last anchors.
final class SyncCoordinator: ObservableObject {

    private let queryService: HealthKitQueryService
    private let apiService: APIService
    private let syncStateRepository: SyncStateRepository

    @Published var lastSyncDate: Date?
    @Published var isSyncing = false
    @Published var lastSyncRecordCount = 0

    init(
        healthStore: HKHealthStore,
        apiService: APIService = APIService(),
        syncStateRepository: SyncStateRepository = SyncStateRepository()
    ) {
        self.queryService = HealthKitQueryService(healthStore: healthStore)
        self.apiService = apiService
        self.syncStateRepository = syncStateRepository
    }

    /// Run a full sync cycle for all configured HealthKit types.
    /// Each type is queried, posted, and its anchor persisted independently.
    func sync() async {
        await MainActor.run { isSyncing = true }
        defer { Task { @MainActor in isSyncing = false } }

        var totalRecords = 0

        // Quantity types — one POST per type (stepCount, heartRate, etc.)
        for quantityType in HealthKitConfiguration.quantityTypes {
            totalRecords += await syncSampleType(quantityType, recordType: "samples") { samples in
                HealthSampleMapper.mapQuantitySamples(samples)
            }
        }

        // Stand hour — category type, mapped as a HealthSample
        let standHourType = HKCategoryType(.appleStandHour)
        totalRecords += await syncSampleType(standHourType, recordType: "samples") { samples in
            HealthSampleMapper.mapCategorySamples(samples)
        }

        // Workouts
        let workoutType = HKSeriesType.workoutType()
        totalRecords += await syncSampleType(workoutType, recordType: "workouts") { samples in
            WorkoutMapper.mapWorkouts(samples)
        }

        // Sleep — stage samples grouped into sessions
        let sleepType = HKCategoryType(.sleepAnalysis)
        totalRecords += await syncSampleType(sleepType, recordType: "sleep") { samples in
            SleepMapper.mapSleepSamples(samples)
        }

        // Activity summaries — date-range query (no anchored query support)
        totalRecords += await syncActivitySummaries()

        await MainActor.run {
            lastSyncDate = Date()
            lastSyncRecordCount = totalRecords
        }
    }

    // MARK: - Per-type batched sync

    /// Generic batched sync for any anchored sample type.
    ///
    /// Loops in batches of `HealthKitConfiguration.queryBatchSize`:
    /// 1. Query up to N samples from the current anchor
    /// 2. Map HKSample objects to Encodable models via the provided transform
    /// 3. POST as NDJSON (~125 KB per batch — fast even on cellular)
    /// 4. Persist the new anchor
    /// 5. If the batch was full (count == limit), there may be more — loop again
    ///
    /// Each batch is an independent commit point. If a POST fails or the background
    /// window expires mid-loop, all previously completed batches are already persisted.
    ///
    /// Returns the total number of records uploaded across all batches.
    private func syncSampleType<T: Encodable>(
        _ sampleType: HKSampleType,
        recordType: String,
        transform: ([HKSample]) -> [T]
    ) async -> Int {
        var currentAnchor = syncStateRepository.anchor(for: sampleType)
        var totalUploaded = 0
        let batchSize = HealthKitConfiguration.queryBatchSize

        while true {
            // Query the next batch
            let result: (samples: [HKSample], deletedObjects: [HKDeletedObject], newAnchor: HKQueryAnchor?)
            do {
                result = try await queryService.fetchNewSamples(
                    for: sampleType,
                    anchor: currentAnchor,
                    limit: batchSize
                )
            } catch {
                Log.sync.error("\(sampleType.identifier): query failed — \(error.localizedDescription)")
                break
            }

            let mapped = transform(result.samples)

            if mapped.isEmpty {
                // No records in this batch — advance anchor and we're done for this type.
                if let newAnchor = result.newAnchor {
                    syncStateRepository.saveAnchor(newAnchor, for: sampleType)
                }
                break
            }

            // POST this batch
            do {
                let response = try await apiService.postRecords(mapped, recordType: recordType)
                Log.sync.info("\(sampleType.identifier): batch uploaded \(mapped.count) records — \(response.status)")

                // Persist anchor immediately — this batch is committed.
                if let newAnchor = result.newAnchor {
                    syncStateRepository.saveAnchor(newAnchor, for: sampleType)
                    currentAnchor = newAnchor
                }
                totalUploaded += mapped.count
            } catch {
                Log.sync.error("\(sampleType.identifier): batch upload failed (\(mapped.count) records) — \(error.localizedDescription)")
                // Stop looping — previously completed batches are safe.
                break
            }

            // If we got fewer than the limit, there's no more data for this type.
            if result.samples.count < batchSize {
                break
            }
        }

        return totalUploaded
    }

    // MARK: - Activity summaries (rings)

    /// Fetch activity summaries since the last sync date. Unlike sample types, activity
    /// summaries don't support anchored queries — we track the last sync date instead.
    private func syncActivitySummaries() async -> Int {
        let syncKey = "activity_summaries"
        let startDate = syncStateRepository.lastSyncDate(for: syncKey)
            ?? Calendar.current.date(byAdding: .day, value: -7, to: Date())!
        let endDate = Date()

        do {
            let summaries = try await queryService.fetchActivitySummaries(from: startDate, to: endDate)
            let mapped = ActivitySummaryMapper.mapSummaries(summaries)

            guard !mapped.isEmpty else { return 0 }

            let response = try await apiService.postRecords(mapped, recordType: "activity_summaries")
            Log.sync.info("Activity summaries: uploaded \(mapped.count) records — \(response.status)")

            syncStateRepository.saveLastSyncDate(endDate, for: syncKey)
            return mapped.count
        } catch {
            Log.sync.error("Activity summary sync failed: \(error.localizedDescription)")
            return 0
        }
    }
}
