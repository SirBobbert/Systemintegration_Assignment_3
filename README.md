# Systemintegration_Assignment_3

## Architecture

The system follows a microservices architecture, where different components handle specific functionalities. The microservices include:

1. **Inventory Management Microservice:** Manages the inventory and provides an API endpoint to check inventory status.

2. **Order Status Management Microservice:** Tracks and updates order statuses. Offers an API endpoint to retrieve order statuses.

3. **Supplier Management Microservice:** Handles the purchase history and communicates with suppliers. Provides an API endpoint for updating purchase history.

Communication between microservices is achieved through Kafka, ensuring event-driven interactions.

## Components

- **inventory_management.py:** Implements the Inventory Management microservice.
- **order_status_management.py:** Implements the Order Status Management microservice.
- **supplier_management.py:** Implements the Supplier Management microservice.

## Requirements

- Python 3.11
- Kafka (and zookeeper)
- MySQL
