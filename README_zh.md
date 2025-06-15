# 图循环检测

本项目是一个Java应用程序，旨在检测图中的循环。它从可配置的数据库加载图数据，然后利用深度优先搜索（DFS）算法来识别循环。该应用程序利用并行处理来有效处理大型图。

## 工作原理

1.  **数据加载**：应用程序连接到数据库（可通过 `AppConfig.java` 配置）以获取图数据。它期望一个包含表示有向边的节点对的表。
2.  **图表示**：加载的数据用于构建图的内存邻接表示。
3.  **循环检测**：从图中的每个节点开始执行深度优先搜索（DFS）算法。
4.  **并行处理**：为了加快检测过程，特别是对于具有许多节点的图，该应用程序使用固定大小的线程池来并行运行DFS遍历。

## 配置

所有配置参数都在 `com.graph.config.AppConfig.java` 文件中管理。您需要检查此文件以了解可用的设置。关键配置通常包括：

*   **数据库连接**：数据库的JDBC URL、用户名、密码。
*   **图数据**：表名、源节点列名、目标节点列名。
*   **性能**：线程池大小、数据加载的批量大小（如果适用）。

**`AppConfig.java` 示例（说明性 - 具体请参考实际文件）：**

```java
package com.graph.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
// 根据需要导入其他包

public class AppConfig {

    // 数据库配置
    private static final String DB_URL = "jdbc:your_database_type:your_database_address"; // 例如 jdbc:postgresql://localhost:5432/mydatabase
    private static final String DB_USER = "your_username";
    private static final String DB_PASSWORD = "your_password";

    // 图表配置
    private static final String TABLE_NAME = "graph_edges"; // 存储图边的表名
    private static final String SOURCE_COLUMN = "source_node_id"; // 源节点列名
    private static final String TARGET_COLUMN = "target_node_id"; // 目标节点列名

    // 性能配置
    private static final int THREAD_POOL_SIZE = 10; // 示例值，线程池大小
    private static final int BATCH_SIZE = 1000; // 示例值，如果使用批量加载

    public static Connection getDataSource() throws SQLException {
        // 在实际应用中，考虑使用连接池以获得更好的性能
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
        // 如果实现，这可能用于批量加载数据
        return BATCH_SIZE;
    }

    // 为任何其他配置添加getter方法
}
```

## 构建和运行

### 先决条件

*   已安装Java开发工具包（JDK）（版本取决于项目的 `pom.xml` 或构建配置）。
*   已安装Apache Maven（用于构建项目）。
*   可以访问 `AppConfig.java` 中指定的数据库系统。
*   `AppConfig.java` 中指定的数据库表必须存在并且已填充图数据。

### 构建

1.  克隆存储库。
2.  导航到项目的根目录。
3.  运行Maven构建命令：
    ```bash
    mvn clean package
    ```
    这将编译代码并创建一个JAR文件（通常在 `target/` 目录中）。

### 运行

1.  确保 `AppConfig.java` 文件已使用您的数据库详细信息和图表信息正确配置。
2.  构建项目后，您可以运行该应用程序。主类是 `com.graph.Main`。
    如果Maven构建创建了一个可执行的JAR，您可以这样运行它（将 `your-project-jar-with-dependencies.jar` 替换为实际的JAR名称）：
    ```bash
    java -jar target/your-project-jar-with-dependencies.jar
    ```
    或者，您可以通过Maven或您的IDE直接运行它，确保类路径已正确设置。

## 依赖项

主要依赖项是：

*   **SLF4J**：用于日志记录。
*   **JDBC 驱动程序**：特定于您要连接的数据库（例如，PostgreSQL JDBC 驱动程序，MySQL Connector/J）。这需要在 `pom.xml` 中声明。

（有关依赖项及其版本的完整列表，请检查 `pom.xml` 文件。）

## 如何使用

1.  **设置您的数据库**：创建一个表示您的图的表。该表应至少有两列：一列用于有向边的源节点ID，另一列用于目标节点ID。
2.  **配置 `AppConfig.java`**：
    *   更新 `DB_URL`、`DB_USER` 和 `DB_PASSWORD` 以指向您的数据库。
    *   更新 `TABLE_NAME`、`SOURCE_COLUMN` 和 `TARGET_COLUMN` 以匹配您的图表设置。
    *   根据您的系统能力和图的大小调整 `THREAD_POOL_SIZE`。
3.  如上所述使用Maven**构建项目**。
4.  **运行应用程序**。应用程序将输出日志信息，包括找到的循环数和每个循环中涉及的节点。

---

*此README是根据项目结构和代码分析自动生成的。*
