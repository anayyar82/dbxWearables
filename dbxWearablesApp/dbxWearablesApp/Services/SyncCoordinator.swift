import Foundation
import HealthKit

/// Orchestrates the sync cycle: query HealthKit → build payload → POST to API → update anchors.
final class SyncCoordinator: ObservableObject {

    private let queryService: HealthKitQueryService
    private let apiService: APIService
    private let syncStateRepository: SyncStateRepository
    private let pendingUploadRepository: PendingUploadRepository

    @Published var lastSyncDate: Date?
    @Published var isSyncing = false

    init(
        healthStore: HKHealthStore,
        apiService: APIService = APIService(),
        syncStateRepository: SyncStateRepository = SyncStateRepository(),
        pendingUploadRepository: PendingUploadRepository = PendingUploadRepository()
    ) {
        self.queryService = HealthKitQueryService(healthStore: healthStore)
        self.apiService = apiService
        self.syncStateRepository = syncStateRepository
        self.pendingUploadRepository = pendingUploadRepository
    }

    /// Run a full sync cycle for all configured sample types.
    func sync() async {
        await MainActor.run { isSyncing = true }
        defer { Task { @MainActor in isSyncing = false } }

        // TODO: Iterate over configured types, fetch new samples via anchored queries,
        // transform HKSample objects into Codable models, build HealthPayload,
        // post to API, and persist updated anchors on success.

        await MainActor.run { lastSyncDate = Date() }
    }
}
