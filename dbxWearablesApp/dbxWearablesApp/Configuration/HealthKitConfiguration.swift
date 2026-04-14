import HealthKit

/// Defines which HealthKit data types the app reads and syncs.
enum HealthKitConfiguration {

    /// Quantity types to query from HealthKit.
    static let quantityTypes: Set<HKQuantityType> = [
        HKQuantityType(.stepCount),
        HKQuantityType(.distanceWalkingRunning),
        HKQuantityType(.activeEnergyBurned),
        HKQuantityType(.basalEnergyBurned),
        HKQuantityType(.heartRate),
        HKQuantityType(.restingHeartRate),
        HKQuantityType(.heartRateVariabilitySDNN),
        HKQuantityType(.oxygenSaturation),
        HKQuantityType(.appleExerciseTime),
        HKQuantityType(.appleStandTime),
        HKQuantityType(.vo2Max),
    ]

    /// Category types to query from HealthKit.
    static let categoryTypes: Set<HKCategoryType> = [
        HKCategoryType(.sleepAnalysis),
        HKCategoryType(.appleStandHour),
    ]

    /// All sample types eligible for background delivery.
    static var allSampleTypes: Set<HKSampleType> {
        var types = Set<HKSampleType>()
        types.formUnion(quantityTypes)
        types.formUnion(categoryTypes)
        types.insert(HKSeriesType.workoutType())
        return types
    }

    /// All object types requested during authorization (read-only).
    static var allReadTypes: Set<HKObjectType> {
        var types = Set<HKObjectType>()
        types.formUnion(quantityTypes)
        types.formUnion(categoryTypes)
        types.insert(HKSeriesType.workoutType())
        types.insert(HKObjectType.activitySummaryType())
        return types
    }

    /// Background delivery frequency for observer queries.
    static let backgroundDeliveryFrequency: HKUpdateFrequency = .hourly

    /// Maximum number of samples per anchored query batch.
    /// Each batch becomes one NDJSON POST. At ~250 bytes per sample, 500 records ≈ 125 KB —
    /// fast to serialize and upload even on cellular, well within the ~30s background window.
    /// HealthKit returns a new anchor after each batch so progress is incremental.
    static let queryBatchSize = 500
}
