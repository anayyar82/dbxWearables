import Foundation
import HealthKit
import OSLog

/// Orchestrates the sync cycle: query HealthKit → map to models → serialize NDJSON → POST → update anchors.
///
/// Each record type (samples, workouts, sleep, activity summaries) is synced independently.
/// Anchors are persisted only after a successful upload so failed syncs are automatically retried.
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
    func sync() async {
        await MainActor.run { isSyncing = true }
        defer { Task { @MainActor in isSyncing = false } }

        var totalRecords = 0
        totalRecords += await syncQuantityTypes()
        totalRecords += await syncCategoryTypes()
        totalRecords += await syncWorkouts()
        totalRecords += await syncActivitySummaries()

        await MainActor.run {
            lastSyncDate = Date()
            lastSyncRecordCount = totalRecords
        }
    }

    // MARK: - Quantity types (stepCount, heartRate, etc.)

    /// Query all configured quantity types via anchored queries, map to HealthSample,
    /// POST as NDJSON, and persist anchors on success. Returns the number of records uploaded.
    private func syncQuantityTypes() async -> Int {
        var allSamples: [HealthSample] = []
        var pendingAnchors: [(HKSampleType, HKQueryAnchor)] = []

        for quantityType in HealthKitConfiguration.quantityTypes {
            let currentAnchor = syncStateRepository.anchor(for: quantityType)

            do {
                let result = try await queryService.fetchNewSamples(
                    for: quantityType,
                    anchor: currentAnchor
                )
                let mapped = HealthSampleMapper.mapQuantitySamples(result.samples)
                allSamples.append(contentsOf: mapped)

                if let newAnchor = result.newAnchor {
                    pendingAnchors.append((quantityType, newAnchor))
                }
                Log.sync.info("Fetched \(mapped.count) new samples for \(quantityType.identifier)")
            } catch {
                Log.sync.error("Query failed for \(quantityType.identifier): \(error.localizedDescription)")
            }
        }

        return await postAndPersistAnchors(allSamples, recordType: "samples", pendingAnchors: pendingAnchors)
    }

    // MARK: - Category types (appleStandHour — sleep handled separately)

    /// Query non-sleep category types via anchored queries. Sleep is excluded here
    /// because it requires stage grouping logic handled by syncSleep().
    private func syncCategoryTypes() async -> Int {
        let standHourType = HKCategoryType(.appleStandHour)
        var allSamples: [HealthSample] = []
        var pendingAnchors: [(HKSampleType, HKQueryAnchor)] = []

        let currentAnchor = syncStateRepository.anchor(for: standHourType)

        do {
            let result = try await queryService.fetchNewSamples(
                for: standHourType,
                anchor: currentAnchor
            )
            let mapped = HealthSampleMapper.mapCategorySamples(result.samples)
            allSamples.append(contentsOf: mapped)

            if let newAnchor = result.newAnchor {
                pendingAnchors.append((standHourType, newAnchor))
            }
            Log.sync.info("Fetched \(mapped.count) new stand hour samples")
        } catch {
            Log.sync.error("Stand hour query failed: \(error.localizedDescription)")
        }

        return await postAndPersistAnchors(allSamples, recordType: "samples", pendingAnchors: pendingAnchors)
    }

    // MARK: - Workouts

    /// Query workouts via anchored query, map to WorkoutRecord, POST as NDJSON.
    private func syncWorkouts() async -> Int {
        let workoutType = HKSeriesType.workoutType()
        let currentAnchor = syncStateRepository.anchor(for: workoutType)
        var pendingAnchors: [(HKSampleType, HKQueryAnchor)] = []

        do {
            let result = try await queryService.fetchNewSamples(
                for: workoutType,
                anchor: currentAnchor
            )
            let mapped = WorkoutMapper.mapWorkouts(result.samples)

            if let newAnchor = result.newAnchor {
                pendingAnchors.append((workoutType, newAnchor))
            }
            Log.sync.info("Fetched \(mapped.count) new workouts")

            return await postAndPersistAnchors(mapped, recordType: "workouts", pendingAnchors: pendingAnchors)
        } catch {
            Log.sync.error("Workout query failed: \(error.localizedDescription)")
            return 0
        }
    }

    // MARK: - Sleep

    /// Query sleep analysis via anchored query, group into sessions via SleepMapper,
    /// POST as NDJSON. Each NDJSON line is one SleepRecord (a complete session with stages).
    private func syncSleep() async -> Int {
        let sleepType = HKCategoryType(.sleepAnalysis)
        let currentAnchor = syncStateRepository.anchor(for: sleepType)
        var pendingAnchors: [(HKSampleType, HKQueryAnchor)] = []

        do {
            let result = try await queryService.fetchNewSamples(
                for: sleepType,
                anchor: currentAnchor
            )
            let mapped = SleepMapper.mapSleepSamples(result.samples)

            if let newAnchor = result.newAnchor {
                pendingAnchors.append((sleepType, newAnchor))
            }
            Log.sync.info("Fetched \(mapped.count) sleep sessions from \(result.samples.count) stage samples")

            return await postAndPersistAnchors(mapped, recordType: "sleep", pendingAnchors: pendingAnchors)
        } catch {
            Log.sync.error("Sleep query failed: \(error.localizedDescription)")
            return 0
        }
    }

    // MARK: - Activity summaries (rings)

    /// Fetch activity summaries since the last sync date. Unlike sample types, activity
    /// summaries don't support anchored queries — we track the last sync date instead.
    private func syncActivitySummaries() async -> Int {
        let syncKey = "activity_summaries"
        // On first sync, look back 7 days. On subsequent syncs, start from last sync date.
        let startDate = syncStateRepository.lastSyncDate(for: syncKey)
            ?? Calendar.current.date(byAdding: .day, value: -7, to: Date())!
        let endDate = Date()

        do {
            let summaries = try await queryService.fetchActivitySummaries(from: startDate, to: endDate)
            let mapped = ActivitySummaryMapper.mapSummaries(summaries)
            Log.sync.info("Fetched \(mapped.count) activity summaries")

            guard !mapped.isEmpty else { return 0 }

            let response = try await apiService.postRecords(mapped, recordType: "activity_summaries")
            Log.sync.info("Activity summaries upload succeeded (\(mapped.count)): \(response.status)")

            syncStateRepository.saveLastSyncDate(endDate, for: syncKey)
            return mapped.count
        } catch {
            Log.sync.error("Activity summary sync failed: \(error.localizedDescription)")
            return 0
        }
    }

    // MARK: - Shared upload + anchor persistence

    /// POST an array of records as NDJSON. On success, persist all pending anchors.
    /// Returns the number of records uploaded (0 if empty or on failure).
    private func postAndPersistAnchors<T: Encodable>(
        _ records: [T],
        recordType: String,
        pendingAnchors: [(HKSampleType, HKQueryAnchor)]
    ) async -> Int {
        guard !records.isEmpty else { return 0 }

        do {
            let response = try await apiService.postRecords(records, recordType: recordType)
            Log.sync.info("\(recordType) upload succeeded (\(records.count) records): \(response.status)")

            for (type, anchor) in pendingAnchors {
                syncStateRepository.saveAnchor(anchor, for: type)
            }
            return records.count
        } catch {
            Log.sync.error("\(recordType) upload failed (\(records.count) records): \(error.localizedDescription)")
            return 0
        }
    }
}
