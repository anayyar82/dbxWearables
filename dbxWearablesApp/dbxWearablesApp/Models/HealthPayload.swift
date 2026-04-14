import Foundation

/// Top-level payload posted to the Databricks REST API.
/// Contains all HealthKit data collected since the last sync.
struct HealthPayload: Codable {
    let deviceId: String
    let platform: String
    let appVersion: String
    let uploadedAt: Date
    let samples: [HealthSample]
    let workouts: [WorkoutRecord]
    let activitySummaries: [ActivitySummary]
    let sleepRecords: [SleepRecord]

    enum CodingKeys: String, CodingKey {
        case deviceId = "device_id"
        case platform
        case appVersion = "app_version"
        case uploadedAt = "uploaded_at"
        case samples
        case workouts
        case activitySummaries = "activity_summaries"
        case sleepRecords = "sleep_records"
    }
}
