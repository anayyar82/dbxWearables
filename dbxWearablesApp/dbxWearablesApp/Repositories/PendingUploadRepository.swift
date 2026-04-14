import Foundation

/// Stores payloads that failed to upload so they can be retried later.
/// Uses file-based persistence in the app's documents directory.
final class PendingUploadRepository {

    private let directory: URL

    init() {
        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
        self.directory = docs.appendingPathComponent("PendingUploads", isDirectory: true)
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    }

    /// Save a payload for later retry. File is named by UUID.
    func save(_ payload: HealthPayload) throws {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(payload)
        let file = directory.appendingPathComponent(UUID().uuidString + ".json")
        try data.write(to: file)
    }

    /// Load all pending payloads.
    func loadAll() throws -> [(url: URL, payload: HealthPayload)] {
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        let files = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
        return try files.filter { $0.pathExtension == "json" }.map { url in
            let data = try Data(contentsOf: url)
            let payload = try decoder.decode(HealthPayload.self, from: data)
            return (url, payload)
        }
    }

    /// Remove a pending payload after successful upload.
    func remove(at url: URL) throws {
        try FileManager.default.removeItem(at: url)
    }
}
