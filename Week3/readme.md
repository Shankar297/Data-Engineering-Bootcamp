# Apache Spark Advanced Course

Welcome to the **Apache Spark Advanced Course**. This course provides a comprehensive overview of Spark, covering essential topics for both beginners and experienced data engineers. Below is a detailed breakdown of the topics covered during the sessions.

---

## Week 03: Day 01 - **Spark Fundamentals**

### Topics Covered:

1. **Introduction to Spark**

   - Overview of Spark’s architecture: Driver, Executors, and Plan.
   - History of Big Data Technologies: Evolution from Hadoop and MapReduce to Hive and Spark.
   - Advantages of Spark:
     - Efficient use of RAM.
     - Storage agnosticism: Support for databases, data lakes, and flat files.
     - Massive community support.
   - Scenarios where Spark might not be suitable.

2. **Understanding Spark Architecture**

   - Analogies: Basketball team (Coach = Driver, Players = Executors, Play = Plan).
   - Lazy evaluation and when Spark executes operations.

3. **Configuration and Optimization**

   - Key Driver Settings:
     - `spark.driver.memory`
     - Memory overhead.
   - Key Executor Settings:
     - `spark.executor.memory`
     - Executor cores.
   - Tips for balancing memory and performance.

4. **Joins and Shuffle**

   - Types of Joins:
     - Shuffle Sort Merge Join.
     - Broadcast Hash Join.
     - Bucket Join.
   - Importance of minimizing shuffle operations.
   - Optimizing joins with bucketed tables and powers-of-two partitioning.

5. **Skew Handling**

   - Detecting skew in data.
   - Solutions:
     - Adaptive Query Execution (`spark.sql.adaptive.enabled`).
     - Salting for skewed Group By operations.
     - Partitioning for outlier handling.

6. **Using Spark for Data Integration**

   - Integration with various data sources (e.g., REST APIs, databases).
   - Handling large API responses and efficient data parallelization.

7. **Best Practices**

   - Use of `show`, `take`, and `collect` responsibly.
   - Repartitioning and sorting strategies for efficient file writing.

---

## Week 03: Day 02 - **Advanced Spark Concepts**

### Topics Covered:

1. **Spark Server vs. Spark Notebooks**

   - Comparison of CLI-based Spark Server (`spark-submit`) and Notebooks.
   - Advantages and drawbacks of each approach.
   - Recommendations for production environments.

2. **Data APIs in Spark**

   - Overview of APIs:
     - DataFrame API.
     - Dataset API.
     - Spark SQL.
     - RDD API (rarely used, guidance to avoid).
   - When to use which API based on job requirements.

3. **Temporary Views and Caching**

   - Creating temporary views.
   - Importance of caching to optimize repeated computations.
   - Memory vs. Disk caching.

4. **Handling UDFs**

   - Comparison of PySpark UDFs and Scala Spark UDFs.
   - Performance trade-offs.

5. **Best Practices in Engineering Pipelines**

   - Unit testing and CICD for Spark pipelines.
   - Avoiding bad practices like overusing notebooks for production.

6. **File Compression and Partitioning**

   - Impact of sorting on compression (Run Length Encoding).
   - Strategies for writing partitioned data efficiently.

---

## Week 03: Day 03 - **Hands-On Spark Lab**

### Topics Covered:

1. **Setup and Environment Configuration**

   - Using Docker to set up Spark with Iceberg, MinIO, and MC.
   - Running local Spark sessions.

2. **Practical Examples**

   - Reading and writing data using Spark.
   - Adding and managing partitions for performance improvements.
   - Exploring metadata in Iceberg tables.

3. **Performance Metrics**

   - Analyzing file sizes and partitioning impact.
   - Query optimization techniques using `EXPLAIN`.

4. **Error Handling**

   - Debugging and troubleshooting out-of-memory errors.
   - Preventing pipeline failures with better configuration and coding practices.

5. **Sorting and Repartitioning**

   - Difference between `sort` and `sortWithinPartitions`.
   - Benefits of local sorting vs. global sorting.

6. **Writing Efficient Pipelines**

   - Sorting by low-cardinality fields for better compression.
   - Strategies for reducing storage costs while maintaining performance.

---

## Additional Notes:

- Always validate configurations in a development environment before moving to production.
- Use **powers-of-two partitioning** for bucketed tables to ensure compatibility.
- Regularly monitor jobs with `EXPLAIN` and adapt settings for evolving datasets.

---

### Resources

- [Data Expert Academy](#): Learn more about Spark and related technologies.
- Docker Setup Guide: [Link](#).
- Apache Spark Documentation: [Spark](https://spark.apache.org/).

---

### Acknowledgments

Special thanks to the Data Expert Academy and Zach willson for creating this detailed Spark curriculum. Keep learning and building efficient data pipelines!

