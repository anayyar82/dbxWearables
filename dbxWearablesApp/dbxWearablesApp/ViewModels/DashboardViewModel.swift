import Foundation

/// Drives the main dashboard view with sync status and summary data.
@MainActor
final class DashboardViewModel: ObservableObject {

    private let healthKitManager = HealthKitManager()
    private lazy var syncCoordinator = SyncCoordinator(healthStore: healthKitManager.healthStore)

    @Published var lastSyncDate: Date?
    @Published var lastSyncSampleCount = 0
    @Published var isSyncing = false

    func requestAuthorization() async {
        try? await healthKitManager.requestAuthorization()
    }

    func syncNow() async {
        isSyncing = true
        await syncCoordinator.sync()
        lastSyncDate = syncCoordinator.lastSyncDate
        lastSyncSampleCount = syncCoordinator.lastSyncSampleCount
        isSyncing = false
    }
}
