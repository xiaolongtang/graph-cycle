# Graph Cycle Detection

This project is a Java application designed to detect cycles in a graph. It loads graph data from a configurable database, then utilizes a Depth First Search (DFS) algorithm to identify cycles. The application leverages parallel processing to efficiently handle large graphs.

## How It Works

1.  **Data Loading**: The application connects to a database (configurable via `AppConfig.java`) to fetch graph data. It expects a table containing pairs of nodes representing directed edges.
2.  **Graph Representation**: The loaded data is used to build an in-memory adjacency list representation of the graph.
3.  **Cycle Detection**: A Depth First Search (DFS) algorithm is performed starting from each node in the graph.
4.  **Parallel Processing**: To speed up the detection process, especially for graphs with many nodes, the application uses a fixed-size thread pool to run DFS traversals in parallel.

## Configuration

All configuration parameters are managed in the `com.graph.config.AppConfig.java` file. You'll need to inspect this file to understand the available settings. Key configurations typically include:

*   **Database Connection**: JDBC URL, username, password for the database.
*   **Graph Data**: Table name, source node column name, target node column name.
*   **Performance**: Thread pool size, batch size for data loading (if applicable).

**Example `AppConfig.java` (illustrative - refer to the actual file for specifics):**

```java
package com.graph.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
// Other imports as needed

public class AppConfig {

    // Database Configuration
    private static final String DB_URL = "jdbc:your_database_type:your_database_address";
    private static final String DB_USER = "your_username";
    private static final String DB_PASSWORD = "your_password";

    // Graph Table Configuration
    private static final String TABLE_NAME = "graph_edges";
    private static final String SOURCE_COLUMN = "source_node_id";
    private static final String TARGET_COLUMN = "target_node_id";

    // Performance Configuration
    private static final int THREAD_POOL_SIZE = 10; // Example value
    private static final int BATCH_SIZE = 1000; // Example value for batch loading, if used

    public static Connection getDataSource() throws SQLException {
        // Consider using a connection pool for better performance in a real application
        return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
    }

    public static String getTableName() {
        return TABLE_NAME;
    }

    public static String getSourceColumn() {
        return SOURCE_COLUMN;
    }

    public static String getTargetColumn() {
        return TARGET_COLUMN;
    }

    public static int getThreadPoolSize() {
        return THREAD_POOL_SIZE;
    }

    public static int getBatchSize() {
        // This might be used for loading data in batches if implemented
        return BATCH_SIZE;
    }

    // Add other getters for any other configurations
}
```

## Building and Running

### Prerequisites

*   Java Development Kit (JDK) installed (version will depend on the project's `pom.xml` or build configuration).
*   Apache Maven installed (for building the project).
*   Access to the database system specified in `AppConfig.java`.
*   The database table specified in `AppConfig.java` must exist and be populated with graph data.

### Building

1.  Clone the repository.
2.  Navigate to the project's root directory.
3.  Run the Maven build command:
    ```bash
    mvn clean package
    ```
    This will compile the code and create a JAR file (usually in the `target/` directory).

### Running

1.  Ensure the `AppConfig.java` file is correctly configured with your database details and graph table information.
2.  After building the project, you can run the application. The main class is `com.graph.Main`.
    If the Maven build created an executable JAR, you might run it like this (replace `your-project-jar-with-dependencies.jar` with the actual JAR name):
    ```bash
    java -jar target/your-project-jar-with-dependencies.jar
    ```
    Alternatively, you might run it directly through Maven or your IDE, ensuring the classpath is correctly set up.

## Dependencies

The primary dependencies are:

*   **SLF4J**: For logging.
*   **JDBC Driver**: Specific to the database you are connecting to (e.g., PostgreSQL JDBC Driver, MySQL Connector/J). This needs to be included in the `pom.xml`.

(Check the `pom.xml` file for a complete list of dependencies and their versions.)

## How to Use

1.  **Set up your database**: Create a table that represents your graph. This table should have at least two columns: one for the source node ID and one for the target node ID of a directed edge.
2.  **Configure `AppConfig.java`**:
    *   Update `DB_URL`, `DB_USER`, and `DB_PASSWORD` to point to your database.
    *   Update `TABLE_NAME`, `SOURCE_COLUMN`, and `TARGET_COLUMN` to match your graph table setup.
    *   Adjust `THREAD_POOL_SIZE` based on your system's capabilities and the size of your graph.
3.  **Build the project** using Maven as described above.
4.  **Run the application**. The application will output logging information, including the number of cycles found and the nodes involved in each cycle.

---

*This README was auto-generated based on the project structure and code analysis.*
