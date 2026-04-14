import UIKit
import HealthKit

class AppDelegate: NSObject, UIApplicationDelegate {

    let healthKitManager = HealthKitManager()

    func application(
        _ application: UIApplication,
        didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]? = nil
    ) -> Bool {
        // Register background delivery for HealthKit data types.
        // This must be called at every app launch — registrations do not persist.
        healthKitManager.registerBackgroundDelivery()
        return true
    }
}
