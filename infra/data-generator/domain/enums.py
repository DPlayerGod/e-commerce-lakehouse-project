"""Enums - Constants for the application."""

PAYMENT_METHODS = ["CARD", "APPLE_PAY", "PAYPAL"]
PAYMENT_STATUS = ["PENDING", "SETTLED", "FAILED"]

CARRIERS = ["UPS", "DHL", "FEDEX"]
CURRENCIES = ["USD", "EUR", "GBP"]
COUNTRIES = ["VN", "TH", "ID", "PH", "MY", "SG", "KH", "LA", "MM", "BN"]

# Delivery statuses
DELIVERY_STATUS = ["DELIVERED", "FAILED", "RETURNED"]
DELIVERY_REASONS = {
    "DELIVERED": ["customer_home", "signed", "left_at_door"],
    "FAILED": ["customer_not_home", "address_unclear", "weather_delay", "vehicle_issue"],
    "RETURNED": ["customer_refused", "damaged_in_transit"]
}
