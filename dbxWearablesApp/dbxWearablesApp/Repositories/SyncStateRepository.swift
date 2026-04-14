import Foundation
import HealthKit

/// Persists sync anchors per HealthKit data type for incremental querying.
/// Uses UserDefaults for simplicity; could be migrated to Core Data or a file-based store.
final class SyncStateRepository {

    private let defaults: UserDefaults
    private let anchorKeyPrefix = "sync_anchor_"

    init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    /// Retrieve the stored anchor for a given sample type, or nil if no prior sync.
    func anchor(for sampleType: HKSampleType) -> HKQueryAnchor? {
        guard let data = defaults.data(forKey: anchorKeyPrefix + sampleType.identifier) else {
            return nil
        }
        return try? NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data)
    }

    /// Persist the anchor after a successful sync for a given sample type.
    func saveAnchor(_ anchor: HKQueryAnchor, for sampleType: HKSampleType) {
        guard let data = try? NSKeyedArchiver.archivedData(
            withRootObject: anchor,
            requiringSecureCoding: true
        ) else { return }
        defaults.set(data, forKey: anchorKeyPrefix + sampleType.identifier)
    }
}
