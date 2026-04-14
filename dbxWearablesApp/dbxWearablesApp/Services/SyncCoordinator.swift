import Foundation
import HealthKit
import OSLog

/// Orchestrates the sync cycle: query HealthKit → map to models → serialize NDJSON → POST → update anchors.
final class SyncCoordinator: ObservableObject {

    private let queryService: HealthKitQueryService
    private let apiService: APIService
    private let syncStateRepository: SyncStateRepository

    @Published var lastSyncDate: Date?
    @Published var isSyncing = false
    @Published var lastSyncSampleCount = 0

    init(
        healthStore: HKHealthStore,
        apiService: APIService = APIService(),
        syncStateRepository: SyncStateRepository = SyncStateRepository()
    ) {
        self.queryService = HealthKitQueryService(healthStore: healthStore)
        self.apiService = apiService
        self.syncStateRepository = syncStateRepository
    }

    /// Run a full sync cycle for all configured quantity types.
    ///
    /// For each quantity type:
    /// 1. Load the persisted anchor (nil on first sync → fetches all history)
    /// 2. Run an anchored object query to get new/updated samples since that anchor
    /// 3. Map HKQuantitySample objects to HealthSample models
    /// 4. Collect all samples across types, then POST as a single NDJSON request
    /// 5. On success, persist the new anchors so the next sync is incremental
    func sync() async {
        await MainActor.run { isSyncing = true }
        defer { Task { @MainActor in isSyncing = false } }

        // Phase 1: Query all quantity types and collect samples + new anchors
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

        // Phase 2: POST collected samples as NDJSON
        guard !allSamples.isEmpty else {
            Log.sync.info("No new quantity samples to upload.")
            await MainActor.run {
                lastSyncDate = Date()
                lastSyncSampleCount = 0
            }
            return
        }

        do {
            let response = try await apiService.postSamples(allSamples)
            Log.sync.info("Upload succeeded (\(allSamples.count) samples): \(response.status)")

            // Phase 3: Persist anchors only after successful upload
            for (type, anchor) in pendingAnchors {
                syncStateRepository.saveAnchor(anchor, for: type)
            }

            await MainActor.run {
                lastSyncDate = Date()
                lastSyncSampleCount = allSamples.count
            }
        } catch {
            Log.sync.error("Upload failed (\(allSamples.count) samples): \(error.localizedDescription)")
            // Anchors are NOT persisted — the next sync will re-fetch these samples.
        }
    }
}
