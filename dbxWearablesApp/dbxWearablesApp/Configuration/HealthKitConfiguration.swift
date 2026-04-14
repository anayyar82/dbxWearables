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
}
