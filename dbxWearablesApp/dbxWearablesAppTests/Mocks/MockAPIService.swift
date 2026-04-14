import Foundation
@testable import dbxWearablesApp

/// Mock API service for unit testing the sync coordinator without network calls.
final class MockAPIService {

    var postResult: Result<APIResponse, Error> = .success(
        APIResponse(status: "ok", message: "Ingested", recordId: "mock-record-id")
    )

    var postedPayloads: [HealthPayload] = []

    func postHealthPayload(_ payload: HealthPayload) async throws -> APIResponse {
        postedPayloads.append(payload)
        return try postResult.get()
    }
}
