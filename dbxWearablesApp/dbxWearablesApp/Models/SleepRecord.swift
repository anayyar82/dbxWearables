import Foundation

/// A sleep session with individual stage intervals.
struct SleepRecord: Codable {
    let startDate: Date
    let endDate: Date
    let stages: [SleepStage]

    enum CodingKeys: String, CodingKey {
        case startDate = "start_date"
        case endDate = "end_date"
        case stages
    }
}

/// A single sleep stage interval within a SleepRecord.
struct SleepStage: Codable {
    let stage: String
    let startDate: Date
    let endDate: Date

    enum CodingKeys: String, CodingKey {
        case stage
        case startDate = "start_date"
        case endDate = "end_date"
    }
}
