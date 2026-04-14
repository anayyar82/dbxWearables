import Foundation
import HealthKit

/// Executes HealthKit queries (anchored object, statistics, activity summary).
final class HealthKitQueryService {

    private let healthStore: HKHealthStore

    init(healthStore: HKHealthStore) {
        self.healthStore = healthStore
    }

    /// Fetch new samples since the given anchor using an anchored object query.
    /// Returns the new samples and an updated anchor to persist for incremental sync.
    func fetchNewSamples(
        for sampleType: HKSampleType,
        anchor: HKQueryAnchor?
    ) async throws -> (samples: [HKSample], deletedObjects: [HKDeletedObject], newAnchor: HKQueryAnchor?) {
        try await withCheckedThrowingContinuation { continuation in
            let query = HKAnchoredObjectQuery(
                type: sampleType,
                predicate: nil,
                anchor: anchor,
                limit: HKObjectQueryNoLimit
            ) { _, added, deleted, newAnchor, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: (added ?? [], deleted ?? [], newAnchor))
                }
            }
            healthStore.execute(query)
        }
    }

    /// Fetch activity summaries for a date range.
    func fetchActivitySummaries(
        from startDate: Date,
        to endDate: Date
    ) async throws -> [HKActivitySummary] {
        let calendar = Calendar.current
        let startComponents = calendar.dateComponents([.year, .month, .day], from: startDate)
        let endComponents = calendar.dateComponents([.year, .month, .day], from: endDate)

        let predicate = HKQuery.predicate(
            forActivitySummariesBetweenStart: startComponents,
            end: endComponents
        )

        return try await withCheckedThrowingContinuation { continuation in
            let query = HKActivitySummaryQuery(predicate: predicate) { _, summaries, error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume(returning: summaries ?? [])
                }
            }
            healthStore.execute(query)
        }
    }
}
