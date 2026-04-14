import XCTest
@testable import dbxWearablesApp

final class HealthPayloadEncodingTests: XCTestCase {

    func testPayloadEncodesToJSON() throws {
        let payload = HealthPayload(
            deviceId: "test-device-id",
            platform: "apple_healthkit",
            appVersion: "1.0.0",
            uploadedAt: Date(timeIntervalSince1970: 1_700_000_000),
            samples: [],
            workouts: [],
            activitySummaries: [],
            sleepRecords: []
        )

        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(payload)
        let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]

        XCTAssertEqual(json?["device_id"] as? String, "test-device-id")
        XCTAssertEqual(json?["platform"] as? String, "apple_healthkit")
    }
}
