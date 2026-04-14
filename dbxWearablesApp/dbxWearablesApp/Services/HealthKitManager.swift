import Foundation
import HealthKit

/// Central manager for HealthKit store access, authorization, and background delivery registration.
final class HealthKitManager: ObservableObject {

    let healthStore = HKHealthStore()

    @Published var isAuthorized = false

    /// Request read-only authorization for all configured HealthKit types.
    /// Note: HealthKit does not reveal which types the user actually granted — `success`
    /// only indicates the authorization dialog was presented.
    func requestAuthorization() async throws {
        guard HKHealthStore.isHealthDataAvailable() else { return }

        try await healthStore.requestAuthorization(
            toShare: [],
            read: HealthKitConfiguration.allReadTypes
        )
        await MainActor.run { isAuthorized = true }
    }

    /// Register background delivery for all configured sample types.
    /// Must be called at every app launch — registrations do not persist across terminations.
    func registerBackgroundDelivery() {
        guard HKHealthStore.isHealthDataAvailable() else { return }

        for sampleType in HealthKitConfiguration.allSampleTypes {
            healthStore.enableBackgroundDelivery(
                for: sampleType,
                frequency: HealthKitConfiguration.backgroundDeliveryFrequency
            ) { success, error in
                if let error {
                    print("Background delivery registration failed for \(sampleType.identifier): \(error.localizedDescription)")
                }
            }
        }
    }
}
