# Databricks Certified Data Engineer Professional Exam Questions

## Question 1

An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code:
```python
 df = spark.read.format("parquet").load(f"/mnt/source/(date)")
```

Which code block should be used to create the date Python variable used in the above code block?

- **A.** date = spark.conf.get("date")
- **B.** input_dict = input() date= input_dict["date"]
- **C.** import sys date = sys.argv[1]
- **D.** date = dbutils.notebooks.getParam("date")
- **E.** dbutils.widgets.text("date", "null") date = dbutils.widgets.get("date")

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Job parameters surface as widgets; creating a widget and reading it via `dbutils.widgets.get` materializes the value that the Jobs API passed.

**Reference:** https://docs.databricks.com/aws/en/jobs/parameter-use

</details>

---

## Question 2

A Delta table of weather records is partitioned by date and has the below schema: 
`date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOAT  `
To find all the records from within the Arctic Circle, you execute a query with the below filter: `latitude > 66.3 ` 
Which statement describes how the Delta engine identifies which files to load?

- **A.** All records are cached to an operational database and then the filter is applied
- **B.** The Parquet file footers are scanned for min and max statistics for the latitude column
- **C.** All records are cached to attached storage and then the filter is applied
- **D.** The Delta log is scanned for min and max statistics for the latitude column
- **E.** The Hive metastore is scanned for min and max statistics for the latitude column

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Delta keeps column-level min/max statistics in the transaction log and uses those stats for data skipping before touching Parquet files.

**Reference:** https://docs.databricks.com/aws/en/delta/data-skipping

</details>

---

## Question 3

The data engineering team needs to connect to an external database that lacks a native Databricks connector. The external system already enforces data security through group membership, and those groups map one-to-one with Databricks user groups.

A new login credential exists for each group, and the Databricks Utilities Secrets module will surface those credentials to engineers inside notebooks.

Assuming the external credentials and Databricks group mapping are correct, which configuration grants each team the minimum required access to its credentials?

- **A.** "Manage" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
- **B.** "Read" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
- **C.** "Read" permissions should be set on a secret scope containing only those credentials that will be used by a given team.
- **D.** "Manage" permissions should be set on a secret scope containing only those credentials that will be used by a given team.  No additional configuration is necessary as long as all users are configured as administrators in the workspace where secrets have been added.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Permissions are applied at the secret-scope level; granting a team read access to the scope that holds its credentials is the narrowest privilege that still lets notebooks resolve the secrets.

**Reference:** https://docs.databricks.com/en/security/secrets/secrets.html#manage-access

</details>

---

## Question 4

Which indicators would you look for in the Spark Ul's Storage tab to signal that a cached table is not performing optimally? Assume you are using Spark's MEMORY_ONLY storage level.

- **A.** Size on Disk is < Size in Memory
- **B.** The RDD Block Name includes the “*" annotation signaling a failure to cache
- **C.** Size on Disk is > 0
- **D.** The number of Cached Partitions > the number of Spark Partitions
- **E.** On Heap Memory Usage is within 75% of Off Heap Memory Usage

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** With the `MEMORY_ONLY` storage level Spark should never spill to disk, so `Size on Disk` is expected to stay at `0`. Any bytes reported there mean partitions were evicted and written out, which shows the cache is not performing optimally.

**Reference:** Spark documentation – *Monitoring and Instrumentation: Storage tab* (https://spark.apache.org/docs/latest/monitoring.html#storage-tab)

</details>

---

## Question 5

What is the first line of a Databricks Python notebook when viewed in a text editor?

- **A.** %python
- **B.** # Databricks notebook source
- **C.** dbutils.notebook.entry_point.getDbutils()

<details><summary>Answer</summary>

**Answer:** %python

**Explanation:** When exported as source code, Databricks inserts the `%python` magic at the top of a Python notebook to declare the language of the following cells.

**Reference:** https://docs.databricks.com/en/notebooks/notebook-export.html

</details>

---

## Question 6

Which statement describes a key benefit of an end-to-end test?

- **A.** Makes it easier to automate your test suite
- **B.** Pinpoints errors in the building blocks of your application
- **C.** Provides testing coverage for all code paths and branches
- **D.** Closely simulates real world usage of your application
- **E.** Ensures code is optimized for a real-life workflow

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** End-to-end tests exercise the entire workflow from inputs through outputs, so they best approximate how a user experiences the system.

**Reference:** https://martinfowler.com/articles/practical-test-pyramid.html#EndToEndTests

</details>

---

## Question 7

The Databricks CLI is used to trigger a run of an existing job by passing the job_id parameter. The response that the job run request has been submitted successfully includes a field run_id.  Which statement describes what the number alongside this field represents?

- **A.** The job_id and number of times the job has been run are concatenated and returned.
- **B.** The total number of jobs that have been run in the workspace.
- **C.** The number of times the job definition has been run in this workspace.
- **D.** The job_id is returned in this field.
- **E.** The globally unique ID of the newly triggered run.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** `run_id` in the Jobs API is a globally unique identifier for that specific job run and lets you poll status or fetch output later.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#get-run

</details>

---

## Question 8

The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE.

The following code correctly imports the production model, loads the customers table containing the customer_id key column into a DataFrame, and defines the feature columns needed for the model.

```python
model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time _since_last_seen", "app_rating"]
```

Which code block will output a DataFrame with the schema "customer_id LONG, predictions DOUBLE"?

- **A.** df.map(lambda x:model(x[{columns])).select("customer_id, predictions")
- **B.** df.select("customer_id", model(*columns).alias("predictions"))
- **C.** model.predict(df, columns)
- **D.** df.select("customer_id", pandas_udf(model, columns).alias("predictions"))
- **E.** df.apply(model, columns).select("customer_id, predictions")

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Registering the MLflow model as a Spark UDF lets you call `model(*columns)` inside a `select`. That UDF returns the DOUBLE prediction column, allowing you to project `customer_id` alongside the predictions in one DataFrame.

**Reference:** https://docs.databricks.com/en/mlflow/models.html#use-mlflow-models-in-spark

</details>

---

## Question 9

Anightly batch job is configured to ingest all data files from a cloud object storage container where records are stored in a nested directory structure YYYY/MM/DD. The data for each date represents all records that were processed by the source system on that date, noting that some records may be delayed as they await moderator approval. Each entry represents a user review of a product and has the following schema:  user_id STRING, review_id BIGINT, product_id BIGINT, review_timestamp TIMESTAMP, review_text STRING  The ingestion job is configured to append all data for the previous date to a target table reviews_raw with an identical schema to the source system. The next step in the pipeline is a batch write to propagate all new records inserted into reviews_raw to a table where data is fully deduplicated, validated, and enriched.  Which solution minimizes the compute costs to propagate this batch of data?

- **A.** Perform a batch read on the reviews_raw table and perform an insert-only merge using the natural composite key user_id, review_id, product_id, review_timestamp.
- **B.** Configure a Structured Streaming read against the reviews_raw table using the trigger once execution mode to process new records as a batch job.
- **C.** Use Delta Lake version history to get the difference between the latest version of reviews_raw and one version prior, then write these records to the next table.
- **D.** Filter all records in the reviews_raw table based on the review_timestamp; batch append those records produced in the last  48 hours.
- **E.** Reprocess all records in reviews_raw and overwrite the next table in the pipeline.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Because only the newly appended data needs re-processing, reading the previous Delta version and diffing it with the latest version (using `table_changes`/time-travel) minimizes compute.

**Reference:** https://docs.databricks.com/en/delta/delta-time-travel.html#retrieve-table-history

</details>

---

## Question 10

Which statement describes Delta Lake optimized writes?

- **A.** Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- **B.** An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- **C.** Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging  bus in one batch once the job is complete.
- **D.** Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- **E.** A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Delta optimized writes reshuffle data so that each partition writes larger, coalesced files, reducing the small-file problem without running OPTIMIZE explicitly.

**Reference:** https://docs.databricks.com/en/delta/optimize.html#optimized-writes

</details>

---

## Question 11

Which statement describes the default execution mode for Databricks Auto Loader?

- **A.** Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; the target table is materialized by directly querying all valid files in the source directory.
- **B.** New files are identified by listing the input directory; the target table is materialized by directly querying all valid files in the source directory.
- **C.** Webhooks trigger a Databricks job to run anytime new data arrives in a source directory; new data are automatically merged into target tables using rules inferred from the data.
- **D.** New files are identified by listing the input directory; new files are incrementally and idempotently loaded into the target Delta Lake table.
- **E.** Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; new files are incrementally and idempotently loaded into the target Delta Lake table.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Auto Loader’s default listing mode repeatedly lists the source directory and incrementally ingests only the new files it discovers.

**Reference:** https://docs.databricks.com/en/ingestion/auto-loader/index.html#file-discovery-modes

</details>

---

## Question 12

A Delta Lake table representing metadata about content posts from users has the following schema: user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE Based on the above schema, which column is a good candidate for partitioning the Delta Table?

- **A.** post_time
- **B.** latitude
- **C.** post_id
- **D.** user_id
- **E.** date

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** The `date` column has natural low cardinality and aligns with common query predicates, making it the appropriate Delta partition column.

**Reference:** https://docs.databricks.com/en/delta/delta-best-practices.html#choose-the-right-partition-column

</details>

---

## Question 13

The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings. The team processes the previous week’s deletions every Sunday at 1 a.m.; the job finishes in under an hour. Every Monday at 3 a.m., a batch job runs VACUUM against every Delta table in the organization.

The compliance officer recently learned about Delta Lake’s time-travel feature and is worried deleted data might still be accessible. Assuming the delete logic is correct, which statement best addresses this concern?

- **A.** Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.
- **B.** Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
- **C.** Because Delta Lake time travel provides full access to the entire history of a table, deleted records can always be recreated by users with full admin privileges.
- **D.** Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.
- **E.** Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Unless you override retention, Delta keeps deleted-data files for seven days, so the Sunday deletes remain accessible via time travel until the Monday one-week-later VACUUM.

**Reference:** https://docs.databricks.com/en/delta/delta-optimize.html#vacuum

</details>

---

## Question 14

A large company seeks to implement a near real-time solution involving hundreds of pipelines with parallel updates of many tables with extremely high volume and high velocity data.  Which of the following solutions would you implement to achieve this requirement?

- **A.** Use Databricks High Concurrency clusters, which leverage optimized cloud storage connections to maximize data throughput.
- **B.** Partition ingestion tables by a small time duration to allow for many data files to be written in parallel.
- **C.** Configure Databricks to save all data to attached SSD volumes instead of object storage, increasing file I/O significantly.
- **D.** Isolate Delta Lake tables in their own storage containers to avoid API limits imposed by cloud vendors.
- **E.** Store all tables in a single database to ensure that the Databricks Catalyst Metastore can load balance overall throughput.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Partitioning ingestion tables by a small time slice lets many writers append concurrently without contending on the same files, which is crucial for hundreds of parallel near-real-time streams.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#partition-large-tables

</details>

---

## Question 15

Which describes a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

- **A.** Run source env/bin/activate in a notebook setup script
- **B.** Use b in a notebook cell
- **C.** Use %pip install in a notebook cell
- **D.** Use %sh pip install in a notebook cell
- **E.** Install libraries from PyPI using the cluster UI

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `%pip install` runs in notebook scope and syncs the installed package to every node attached to the current cluster.

**Reference:** https://docs.databricks.com/en/libraries/notebooks-python-libraries.html#python-pip-install

</details>

---

## Question 16

Each configuration below is identical to the extent that each cluster has 400 GB total of RAM 160 total cores and only one Executor per VM.  Given an extremely long-running job for which completion must be guaranteed, which cluster configuration will be able to guarantee completion of the job in light of one or more VM failures?

- **A.** + Total VMs: 8 « 50 GB per Executor + 20 Cores / Executor
- **B.** + Total VMs: 16 - 25 GB per Executor - 10 Cores / Executor
- **C.** + Total VMs: 1 + 400 GB per Executor - 160 Cores/Executor
- **D.** + Total VMs: 4 - 100 GB per Executor + 40 Cores / Executor
- **E.** + Total VMs: 2 - 200 GB per Executor + 80 Cores / Executor

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** More, smaller VMs (16 in this case) allow Spark to survive the loss of one executor without losing all cores or memory, which is critical for ultra-long jobs.

**Reference:** https://docs.databricks.com/en/clusters/configure.html#choose-the-right-number-of-workers

</details>

---

## Question 17

A Delta Lake table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.  Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.  Given the current implementation, which method can be used?

- **A.** Execute a query to calculate the difference between the new version and the previous version using Delta Lake's built-in versioning and lime travel functionality.
- **B.** Parse the Delta Lake transaction log to identify all newly written data files.
- **C.** Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.
- **D.** Execute DESCRIBE HISTORY customer_churn_params to obtain the full operation metrics for the update, including a log of  all records that have been added or modified.
- **E.** Use Delta Lake's change data feed to identify those records that have been updated, inserted, or deleted.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Because Delta automatically versions every overwrite, you can query `VERSION AS OF` (or `TIMESTAMP AS OF`) to diff two versions immediately after the nightly load.

**Reference:** https://docs.databricks.com/en/delta/delta-time-travel.html#query-an-earlier-version-of-the-table

</details>

---

## Question 18

A data team maintains a Structured Streaming job that computes running aggregates for item sales. Marketing now wants to track how many times a specific promotion code is used per item. A junior engineer proposes updating the query (new logic commented):

```python
df.groupBy("item")
  .agg(
      count("item").alias("total_count"),
      mean("sale_price").alias("avg_price"),
      count("promo_code = 'NEW_MEMBER'").alias("new_member_promo") # NEW LOGIC
  )
  .writeStream
  .outputMode("complete")
  .option("mergeSchema", "true") # NEW LOGIC
  .option("checkpointLocation", "/item_agg/__checkpoint")
  .start("/item_agg")
```

Which additional step is required before moving this change into production?

- **A.** Specify a new checkpointLocation
- **B.** Remove .option('mergeSchema', 'true') from the streaming write
- **C.** Increase the shuffle partitions to account for additional aggregates
- **D.** Run REFRESH TABLE delta.'/item_agg'

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Changing the query schema means the existing checkpoint metadata no longer matches the streaming plan. Using a brand-new checkpoint directory prevents corrupt state when restarting the job.

**Reference:** https://docs.databricks.com/aws/en/structured-streaming/checkpoints

</details>

---

## Question 19

When using the Databricks CLI or Jobs REST API to fetch results for multi-task jobs, which statement best describes the response structure?

- **A.** Each run of a job has a unique job_id; every task within that run reuses the same job_id.
- **B.** Each run of a job has a unique job_id; every task within that run has its own task_id.
- **C.** Each run of a job has a unique orchestration_id; every task has its own run_id.
- **D.** Each run of a job has a unique run_id; every task within that run has its own task_id.
- **E.** Each run of a job has a unique run_id; every task within that run shares the same run_id.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The Jobs REST API returns a unique `run_id` for the overall job run, while each task section exposes its own `task_run_id` (task_id) so you can inspect per-task state or output.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#get-run

</details>

---

## Question 20

The data engineering team is configuring environments for development, testing, and production before migrating a new pipeline. They need realistic test data but want to minimize the risk of modifying production data. A junior engineer suggests mounting production data into dev and test workspaces because everyone there has admin privileges and can manage permissions.

Which response reflects best practices?

- **A.** All code and data should live in one workspace; separate environments add needless overhead.
- **B.** In interactive environments, production data should be read-only and isolated; using separate databases per environment lowers risk.
- **C.** As long as notebooks start with `USE dev_db`, changes can’t accidentally hit production sources.
- **D.** Delta time travel prevents permanent deletion, so mounting production data everywhere is safe.
- **E.** Passthrough authentication guarantees safety, so production data can be mounted in any Databricks workspace.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Databricks recommends strict isolation between production and pre-production environments; if production data must be referenced, it should be read-only and separated into distinct schemas or databases to prevent accidental writes.

**Reference:** https://docs.databricks.com/en/data-governance/workspace-best-practices.html

</details>

## Question 21

A data engineer, User A, has promoted a pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.  A workspace admin, User C, inherits responsibility for managing this pipeline. User C uses the Databricks Jobs UI to take "Owner" privileges of each job. Jobs continue to be triggered using the credentials and tooling configured by User B.  An application has been configured to collect and parse run information returned by the REST API. Which statement describes the value returned in the creator_user_name field?

- **A.** Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User A’s email address will appear in this field.
- **B.** User B's email address will always appear in this field, as their credentials are always used to trigger the run.
- **C.** User A's email address will always appear in this field, as they still own the underlying notebooks.
- **D.** Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User B’s email address will  appear in this field.
- **E.** User C will only ever appear in this field if they manually trigger the job, otherwise it will indicate User B.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The `creator_user_name` recorded for a run reflects the identity behind the personal access token that triggered it, so it continues to show User B even after ownership changes.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#get-run



</details>

---

## Question 22

A member of the data engineering team has submitted a short notebook that they want to run as a scheduled job. The notebook currently contains the following cells:

```
Cmd 1  rawDF = spark.table("raw_data")
Cmd 2  rawDF.printSchema()
Cmd 3  flattenedDF = rawDF.select("*", "values.*")
Cmd 4  finalDF = flattenedDF.drop("values")
Cmd 5  finalDF.explain()
Cmd 6  display(finalDF)
Cmd 7  finalDF.write.mode("append").saveAsTable("flat_data")
```

Which command should be removed before turning this into a job so that the pipeline runs without an interactive notebook UI?

- **A.** Cmd 2
- **B.** Cmd 3
- **C.** Cmd 4
- **D.** Cmd 5
- **E.** Cmd 6

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Jobs execute in a headless environment, so interactive helpers such as `display()` must be removed; instead, persist results to storage for later review.

**Reference:** https://docs.databricks.com/en/notebooks/notebook-best-practices.html#use-display-only-for-ad-hoc-analysis



</details>

---

## Question 23

Which statement regarding Spark configuration on the Databricks platform is true?

- **A.** The Databricks REST API can be used to modify the Spark configuration properties for an interactive cluster without interrupting jobs currently running on the cluster.
- **B.** Spark configurations set within a notebook will affect all SparkSessions attached to the same interactive cluster.
- **C.** When the same Spark configuration property is set for an interactive cluster and a notebook attached to that cluster, the notebook setting will always be ignored.
- **D.** Spark configuration properties set for an interactive cluster with the Clusters UI will impact all notebooks attached to that cluster.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Cluster-scoped Spark configuration set through the Clusters UI applies to every notebook attached to that interactive cluster.

**Reference:** https://docs.databricks.com/en/clusters/configure.html#spark-configuration



</details>

---

## Question 24

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint 2.0/jobs/create.  {  “name": “Ingest new data",  /ingest.py"  Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

- **A.** Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once daily.
- **B.** The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
- **C.** Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.
- **D.** One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
- **E.** The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Calling the Jobs Create API only defines a job; each request creates a new job definition and does not execute it until a separate run request is made.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#create-job



</details>

---

## Question 25

The business reporting team requires that data for their dashboards be updated every hour. The total processing time for the pipeline that extracts, transforms, and loads the data for their pipeline runs in 10 minutes.  Assuming normal operating conditions, which configuration will meet their service-level agreement requirements with the lowest cost?

- **A.** Configure a job that executes every time new data lands in a given directory
- **B.** Schedule a job to execute the pipeline once an hour on a new job cluster
- **C.** Schedule a Structured Streaming job with a trigger interval of 60 minutes
- **D.** Schedule a job to execute the pipeline once an hour on a dedicated interactive cluster

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Running the pipeline hourly on an ephemeral job cluster meets the SLA while incurring compute only for the 10‑minute processing window each hour.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-best-practices.html#use-job-clusters



</details>

---

## Question 26

A Databricks SQL dashboard has been configured to monitor the total number of records present in a collection of Delta Lake tables using the following query pattern:

```
SELECT COUNT (*) FROM table
```

Which of the following describes how results are generated each time the dashboard is updated?

- **A.** The total count of rows is calculated by scanning all data files
- **B.** The total count of rows will be returned from cached results unless REFRESH is run
- **C.** The total count of records is calculated from the Delta transaction logs
- **D.** The total count of records is calculated from the parquet file metadata

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta Lake stores per-file record counts in the transaction log, allowing `COUNT(*)` to be answered from metadata instead of rescanning all Parquet files.

**Reference:** https://docs.databricks.com/en/delta/delta-utility.html#table-details



</details>

---

## Question 27

A Delta Lake table was created with the below query: Consider the following query: DROP TABLE prod.sales_by_store - If this statement is executed by a workspace admin, which result will occur?

- **A.** Data will be marked as deleted but still recoverable with Time Travel.
- **B.** The table will be removed from the catalog but the data will remain in storage.
- **C.** The table will be removed from the catalog and the data will be deleted.
- **D.** An error will occur because Delta Lake prevents the deletion of production data.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Dropping a managed Delta table removes both the metastore entry and the underlying data directory on DBFS object storage.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#drop-a-table



</details>

---

## Question 28

A developer has successfully configured their credentials for Databricks Repos and cloned a remote Git repository. They do not have privileges to make changes to the main branch, which is the only branch currently visible in their workspace.  Which approach allows this user to share their code updates without the risk of overwriting the work of their teammates?

- **A.** Use Repos to create a new branch, commit all changes, and push changes to the remote Git repository.
- **B.** Use Repos to create a fork of the remote repository, commit all changes, and make a pull request on the source repository.
- **C.** Use Repos to pull changes from the remote Git repository; commit and push changes to a branch that appeared as changes were pulled.
- **D.** Use Repos to merge all differences and make a pull request back to the remote repository.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Best practice is to create a feature branch in Repos, commit changes there, and push that branch to the remote repository before opening a pull request.

**Reference:** https://docs.databricks.com/en/repos/git-operations-with-repos.html#create-a-branch



</details>

---

## Question 29

The security team is exploring whether or not the Databricks secrets module can be leveraged for connecting to an external database.  After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and fi the correct permissit for the currently active user. They then modify their code to the following (leaving all other variables unchanged).  Which statement describes what will happen when the above code is executed?

- **A.** The connection to the external table will succeed; the string "REDACTED" will be printed.
- **B.** An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.
- **C.** An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.
- **D.** The connection to the external table will succeed; the string value of password will be printed in plain text.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Databricks redacts secret values when printed, so the connection succeeds but any attempt to print the secret shows `REDACTED`.

**Reference:** https://docs.databricks.com/en/security/secrets/secrets.html#access-secrets



</details>

---

## Question 30

The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a DOUBLE prediction column. The following code imports the production model, loads the `customers` table, and defines the feature columns for scoring:

```python
model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/churn/prod"
)

df = spark.table("customers")

columns = ["account_age", "time_since_last_seen", "app_rating"]
```

Which code block will output a DataFrame with the schema `customer_id LONG, predictions DOUBLE`?

- **A.** df.map(lambda x:model(x[columns])).select(" _id, predictions")
- **B.** df.select("customer_id", model(*columns).alias("predictions"))
- **C.** model.predict(df, columns)
- **D.** df.apply(model, columns).select("customer_id, predictions")

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Registering the MLflow model as a Spark UDF lets you select `customer_id` and call `model(*columns)` to append the DOUBLE prediction column.

**Reference:** https://docs.databricks.com/en/mlflow/models.html#use-mlflow-models-in-spark



</details>

---

## Question 31

A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. They expect the following cells to register a view of all sales that occurred in countries on the continent of Africa found in `geo_lookup`. The current database contains only two tables: `geo_lookup` and `sales`.

```
Cmd 1
%python
countries_af = [x[0] for x in spark.table("geo_lookup")
                                 .filter("continent='AF'")
                                 .select("country")
                                 .collect()]

Cmd 2
%sql
CREATE VIEW sales_af AS
  SELECT *
  FROM sales
  WHERE city IN countries_af
    AND CONTINENT = "AF"
```

What will be the outcome of executing these command cells in order inside an interactive notebook?

- **A.** Both commands will succeed. Executing SHOW TABLES will show that countries_af and sales_af have been registered as views.
- **B.** Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists,  Cmd 2 will succeed.
- **C.** Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
- **D.** Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** The first cell creates a Python DataFrame variable, but SQL cells cannot reference Python variables, so the second cell fails with `TABLE OR VIEW NOT FOUND`.

**Reference:** https://docs.databricks.com/en/notebooks/notebook-workflows.html#pass-values-between-languages



</details>

---

## Question 32

The data science team has requested assistance in accelerating queries on free-form text from user reviews. The data is currently stored in Parquet with the below schema:  item_id INT, user_id INT, review_id INT, rating FLOAT, review STRING  The review column contains the full text of the review left by the user. Specifically, the data science team is looking to identify if any of 30 key words exist in this field.  A junior data engineer suggests converting this data to Delta Lake will improve query performance.  Which response to the junior data engineer's suggestion is correct?

- **A.** Delta Lake statistics are not optimized for free text fields with high cardinality.
- **B.** Delta Lake statistics are only collected on the first 4 columns in a table.
- **C.** ZORDER ON review will need to be run to see performance gains.
- **D.** The Delta log creates a term matrix for free text fields to support selective filtering.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Delta’s stats are optimized for structured columns; high-cardinality free text does not benefit unless you reorganize the data (e.g., ZORDER), so converting alone won’t help.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html#limitations



</details>

---

## Question 33

The data engineering team runs a weekly “forget me” job that deletes customer data from Delta Lake tables using default settings. Each Sunday at 1 a.m. the job deletes all requests from the previous week and finishes within an hour. Every Monday at 3 a.m. another batch job issues VACUUM commands against every Delta table.

After learning about Delta Lake’s time-travel capability, the compliance officer worries that deleted data might remain accessible. Assuming the delete logic is correct, which statement best addresses this concern?

- **A.** Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be  accessible with time travel for around 24 hours.
- **B.** Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
- **C.** Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later.
- **D.** Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage  systems as soon as a delete job completes.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** By default Delta retains data files for seven days, so deleted rows remain accessible via time travel until the next VACUUM after that retention window.

**Reference:** https://docs.databricks.com/en/delta/delta-optimize.html#vacuum



</details>

---

## Question 34

Assuming that the Databricks CLI has been installed and configured correctly, which Databricks CLI command can be used to upload a custom Python Wheel to object storage mounted with the DBFS for use with a production job?

- **A.** configure
- **B.** fs
- **C.** workspace
- **D.** libraries

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Use the `databricks fs cp` (or related) subcommand to copy a local wheel into DBFS so that jobs can install it from object storage.

**Reference:** https://docs.databricks.com/en/dev-tools/cli/index.html#copying-files



</details>

---

## Question 35

An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.  For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.  Which solution meets these requirements?

- **A.** Create a separate history table for each pk_id resolve the current state of the table by running a union all filtering the history tables for the most recent state.
- **B.** Use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a bronze table, then propagate all changes throughout the system.
- **C.** Iterate through an ordered set of changes to the table, applying each in turn; rely on Delta Lake's versioning ability to create an audit log.
- **D.** Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to all dependent tables in the Lakehouse.
- **E.** Ingest all log information into a bronze table; use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a silver table to recreate the current table state.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Landing every CDC record in a bronze table preserves full history, and a MERGE into the silver `account_current` table maintains the latest state per `pk_id`.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#upsert-into-a-table-using-merge



</details>

---

## Question 36

The `carts` table tracks items in user carts and uses schema evolution:

```
Carts (id LONG, email STRING, items ARRAY<STRUCT<id: STRING, count: INT>>)

id    items                                         email
1001  [(id: "DESK65", count: 1)]                    "u1@domain.com"
1002  [(id: "KYBD45", count: 1), (id: "M27", count: 2)]  "u2@domain.com"
1003  [(id: "M27", count: 1)]                       "u3@domain.com"
```

Updates are applied via a Delta MERGE with schema evolution enabled:

```
MERGE INTO carts c
USING updates u
ON c.id = u.id
WHEN MATCHED THEN UPDATE SET *
```

How will the following update be handled when MERGE runs?

```
id    items
1001  [(id: "DESK65", count: 2, coupon: "BOGO50")]
```

- **A.** The update throws an error because changes to existing columns in the target schema are not supported.
- **B.** The new nested field is added to the target schema, and existing rows read `NULL` for coupon.
- **C.** The update is moved to a rescued column because coupon does not exist in the target schema.
- **D.** The new nested field is added and Delta rewrites existing files to include coupon with `NULL`.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** With schema evolution enabled, Delta automatically adds the new nested field and populates `NULL` for previously written rows.

**Reference:** https://docs.databricks.com/en/delta/schema-evolution.html



</details>

---

## Question 37

An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.  For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.  Which solution meets these requirements?

- **A.** Iterate through an ordered set of changes to the table, applying each in turn to create the current state of the table, (insert, update, delete), timestamp of change, and the values.
- **B.** Use merge into to insert, update, or delete the most recent entry for each pk_id into a table, then propagate all changes throughout the system.
- **C.** Deduplicate records in each batch by pk_id and overwrite the target table.
- **D.** Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to  all dependent tables in the Lakehouse.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** MERGE operations can apply insert/update/delete actions per `pk_id`, keeping the current table synchronized while upstream CDC logs maintain history.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html#use-merge-for-cdc



</details>

---

## Question 38

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema:  user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT  New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.  Which implementation can be used to efficiently update the described account_current table as part of each hourly batch job assuming there are millions of user accounts and tens of thousands of records processed hourly?

- **A.** Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.
- **B.** Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger available job to batch update newly detected files into the account_current table.
- **C.** Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- **D.** Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max  last_login by user_id write a merge statement to update or insert the most recent value for each user_id.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Filtering the latest hour’s changes and MERGEing them into the Type 1 table updates only the affected `user_id` rows, avoiding full-table rewrites.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#incremental-updates-with-merge



</details>

---

## Question 39

The business intelligence team maintains a dashboard with fields:

```
store_id INT,
total_sales_qtd FLOAT,
avg_daily_sales_qtd FLOAT,
total_sales_ytd FLOAT,
avg_daily_sales_ytd FLOAT,
previous_day_sales FLOAT,
total_sales_7d FLOAT,
avg_daily_sales_7d FLOAT,
updated TIMESTAMP
```

Demand forecasting relies on a near–real-time table `products_per_order` with schema:

```
store_id INT,
order_id INT,
product_id INT,
quantity INT,
price FLOAT,
order_timestamp TIMESTAMP
```

Analysts only need the dashboard refreshed daily, but queries must be fast and inexpensive. Which solution meets these requirements?

- **A.** Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update.
- **B.** Use Structured Streaming to configure a live dashboard against the products_per_order table within a Databricks notebook.
- **C.** Define a view against the products_per_order table and define the dashboard against this view.
- **D.** Use the Delta Cache to persist the products_per_order table in memory to quickly update the dashboard with each query.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Materializing the metrics nightly into a dedicated table lets BI queries read a compact dataset quickly without streaming or repeatedly scanning raw fact tables.

**Reference:** https://docs.databricks.com/en/sql/admin/alerts-dashboards.html#schedule-refreshes



</details>

---

## Question 40

A Delta lake table with CDF enabled table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.  The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.  Which approach would simplify the identification of these changed records?

- **A.** Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
- **B.** Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
- **C.** Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.
- **D.** Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being written; use this field to identify records written on a particular date.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Replacing full overwrites with a MERGE plus Delta’s change data feed limits predictions to the rows whose values actually changed in the past 24 hours.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html



</details>

---

## Question 41

A view is registered with the following SQL:

```sql
CREATE VIEW recent_orders AS
SELECT a.user_id,
       a.email,
       b.order_date
FROM users a
JOIN orders b
  ON a.user_id = b.user_id
WHERE b.order_date >= current_date() - 7;
```

Both `users` and `orders` are Delta tables. Which statement describes the results of querying `recent_orders`?

- **A.** All logic will execute when the view is defined and store the result of joining tables to the DBFS; this stored data will be returned when the view is queried.
- **B.** Results will be computed and cached when the view is defined; these cached results will incrementally update as new records are inserted into source tables.
- **C.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- **D.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Views in Spark SQL are logical; every query reads a consistent snapshot of the underlying Delta tables as of the query start time, so results reflect the data committed when the query begins.

**Reference:** https://docs.databricks.com/en/delta/concurrency-control.html#serializable-isolation



</details>

---

## Question 42

A data ingestion task requires a 1‑TB JSON dataset to be written out to Parquet with a target part-file size of 512 MB. Because Parquet (not Delta) is used, automatic file compaction is unavailable. Which strategy yields the best performance without shuffling?

- **A.** Set `spark.sql.files.maxPartitionBytes` to 512 MB, ingest the data, run the narrow transformations, then write to Parquet.
- **B.** Set `spark.sql.shuffle.partitions` to 2,048, ingest the data, run the transforms, sort (to repartition), then write to Parquet.
- **C.** Set `spark.sql.adaptive.advisoryPartitionSizeInBytes` to 512 MB, ingest, run the transforms, coalesce to 2,048 partitions, then write.
- **D.** Ingest, run the transforms, repartition to 2,048 partitions, then write to Parquet.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `spark.sql.files.maxPartitionBytes` controls how much data is read into each partition without a shuffle, producing ~512‑MB partitions that map directly to Parquet files.

**Reference:** https://spark.apache.org/docs/latest/sql-performance-tuning.html#partitioning



</details>

---

## Question 43

Which statement regarding stream-static joins and static Delta tables is correct?

- **A.** The streaming checkpoint tracks updates to the static Delta table.
- **B.** Each micro-batch uses the version of the static Delta table that was current when the streaming query started.
- **C.** The checkpoint directory stores state information for the distinct keys present in the static table.
- **D.** Stream-static joins cannot read static Delta tables because of consistency issues.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Stream-static joins read a snapshot of the static Delta table when the query starts; each micro-batch reuses that version to guarantee consistency.

**Reference:** https://docs.databricks.com/en/structured-streaming/stream-static-joins.html



</details>

---

## Question 44

A junior data engineer must build a streaming aggregation that computes the average humidity and temperature for each non-overlapping five-minute interval. Events arrive once per minute per device, and the streaming DataFrame has schema `device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT`.

```python
(df.withWatermark("event_time", "10 minutes")
   .groupBy(
       ______,
       "device_id"
   )
   .agg(
       avg("temp").alias("avg_temp"),
       avg("humidity").alias("avg_humidity")
   )
   .writeStream
   .format("delta")
   .saveAsTable("sensor_avg"))
```

Which line should fill the blank?

- **A.** `to_interval("event_time", "5 minutes").alias("time")`
- **B.** `window("event_time", "5 minutes").alias("time")`
- **C.** `"event_time"`
- **D.** `lag("event_time", "10 minutes").alias("time")`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Structured Streaming uses the `window` function to define non-overlapping time buckets; grouping by `window("event_time","5 minutes")` yields five-minute aggregates.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#aggregate-operations



</details>

---

## Question 45

A Structured Streaming job deployed to production has been resulting in higher than expected cloud storage costs. At present, during normal execution, each microbatch of data is processed in less than 3s; at least 12 times per minute, a microbatch is processed that contains 0 records. The streaming write was configured using the default trigger settings. The production job is currently scheduled alongside many other Databricks jobs in a workspace with instance pools provisioned to reduce start-up time for jobs with batch execution.  Holding all other variables constant and assuming records need to be processed in less than 10 minutes, which adjustment will meet the requirement?

- **A.** Set the trigger interval to 3 seconds; the default trigger interval is consuming too many records per batch, resulting in spill to disk that can increase volume costs.
- **B.** Use the trigger once option and configure a Databricks job to execute the query every 10 minutes; this approach minimizes costs for both compute and storage.
- **C.** Set the trigger interval to 10 minutes; each batch calls APIs in the source storage account, so decreasing trigger frequency to maximum allowable threshold should minimize this cost.
- **D.** Set the trigger interval to 500 milliseconds; setting a small but non-zero trigger interval ensures that the source is not queried too frequently.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Using `trigger(once=True)` and scheduling the job every 10 minutes processes any data that arrived since the last run while avoiding dozens of empty micro-batches and their storage costs.

**Reference:** https://docs.databricks.com/en/structured-streaming/triggers.html#available-triggers



</details>

---

## Question 46

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT  New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.  Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

- **A.** Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
- **B.** Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- **C.** Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_iogin by user_id write a merge statement to update or insert the most recent value for each user_id.
- **D.** Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
- **E.** Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Filtering the most recent hour’s records and merging them into the Type 1 table updates only the affected `user_id` rows, which scales to millions of accounts.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#incremental-updates-with-merge



</details>

---

## Question 47

Which statement describes Delta Lake optimized writes?

- **A.** Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- **B.** An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- **C.** A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.
- **D.** Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta optimized writes introduce a shuffle before files are written so executors emit fewer, larger files instead of many tiny partition-based outputs.

**Reference:** https://docs.databricks.com/en/delta/optimize.html#optimized-writes



</details>

---

## Question 48

Which statement characterizes the general programming model used by Spark Structured Streaming?

- **A.** Structured Streaming leverages the parallel processing of GPUs to achieve highly parallel data throughput.
- **B.** Structured Streaming is implemented as a messaging bus and is derived from Apache Kafka.
- **C.** Structured Streaming relies on a distributed network of nodes that hold incremental state values for cached stages.
- **D.** Structured Streaming models new data arriving in a data stream as new rows appended to an unbounded table.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Structured Streaming models a streaming source as an unbounded table where new data is appended as new rows, enabling SQL-like incremental computation.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#basic-concepts



</details>

---

## Question 49

Which configuration parameter directly affects the size of a spark-partition upon ingestion of data into Spark?

- **A.** spark.sql.files.maxPartitionBytes
- **B.** spark.sql.autoBroadcastJoinThreshold
- **C.** spark.sql.adaptive.advisoryPartitionSizelnBytes
- **D.** spark.sql.adaptive.coalescePartitions.minPartitionNum

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `spark.sql.files.maxPartitionBytes` determines how many input bytes Spark packs into each partition when reading files, directly affecting partition size.

**Reference:** https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options



</details>

---

## Question 50

A Spark job is taking longer than expected. Using the Spark UI, a data engineer notes that the Min, Median, and Max Durations for tasks in a particular stage show the minimum and median time to complete a task as roughly the same, but the max duration for a task to be roughly 100 times as long as the minimum.  Which situation is causing increased duration of the overall job?

- **A.** Task queueing resulting from improper thread pool assignment.
- **B.** Spill resulting from attached volume storage being too small.
- **C.** Network latency due to some cluster nodes being in different regions from the source data
- **D.** Skew caused by more data being assigned to a subset of spark-partitions.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** When some tasks take 100× longer, it usually indicates skew—one or more partitions contain far more data than the others, prolonging the stage.

**Reference:** https://docs.databricks.com/en/optimizations/skew.html



</details>

---

## Question 51

Each configuration below is identical to the extent that each cluster has 400 GB total of RAM, 160 total cores and only one Executor per VM.  Given an extremely long-running job for which completion must be guaranteed, which cluster configuration will be able to guarantee completion of the job in light of one or more VM failures?

- **A.** + Total VMs: 8 « 50 GB per Executor + 20 Cores / Executor
- **B.** + Total VMs: 16 - 25 GB per Executor - 10 Cores / Executor
- **C.** + Total VMs: 1 + 400 GB per Executor - 160 Cores/Executor
- **D.** + Total VMs: 4 - 100 GB per Executor + 40 Cores / Executor

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** More, smaller VMs (16 in this case) provide redundancy; the job can continue even if a VM fails because plenty of executors remain.

**Reference:** https://docs.databricks.com/en/clusters/configure.html#choose-the-right-number-of-workers



</details>

---

## Question 52

A task orchestrator runs two hourly steps. First, an external system writes Parquet files to `/mnt/raw_orders/`. Next, a Databricks job executes:

```python
(spark.readStream
      .format("parquet")
      .load("/mnt/raw_orders/")
      .withWatermark("time", "2 hours")
      .dropDuplicates(["customer_id", "order_id"])
      .writeStream
      .trigger(once=True)
      .table("orders"))
```

`customer_id` and `order_id` form a composite key, and `time` records when the order entered the queue. The upstream system may enqueue duplicate entries for the same order hours apart. Which statement is correct?

- **A.** Duplicate records enqueued more than 2 hours apart may be retained and the orders table may contain duplicate records with the same customer_id and order_id.
- **B.** All records will be held in the state store for 2 hours before being deduplicated and committed to the orders table.
- **C.** The orders table will contain only the most recent 2 hours of records and no duplicates will be present.
- **D.** The orders table will not contain duplicates, but records arriving more than 2 hours late will be ignored and missing from the  table.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Watermarked `dropDuplicates` only removes duplicates that arrive within the watermark. Duplicates separated by more than two hours will both persist in the `orders` table.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking



</details>

---

## Question 53

A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.  In addition to de-duplicating records within the batch, which of the following approaches allows the data engineer to deduplicate data against previously processed records as it is inserted into a Delta table?

- **A.** Rely on Delta Lake schema enforcement to prevent duplicate records.
- **B.** VACUUM the Delta table after each batch completes.
- **C.** Perform an insert-only merge with a matching condition on a unique key.
- **D.** Perform a full outer join on a unique key and overwrite existing data.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Using an insert-only `MERGE` with a unique key lets Delta Lake check existing rows and skip duplicates already written in previous batches.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#upsert-into-a-table-using-merge



</details>

---

## Question 54

A junior data engineer wants to use Delta Change Data Feed (CDF) to build a Type 1 history table for a bronze table created with `delta.enableChangeDataFeed = true`. They schedule the following daily job:

```python
from pyspark.sql.functions import col

(spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .table("bronze")
      .filter(col("_change_type").isin(["update_postimage", "insert"]))
      .write
      .mode("append")
      .table("bronze_history_type1"))
```

Which statement describes the effect of running this code multiple times?

- **A.** Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with  the same primary keys.
- **B.** Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.
- **C.** Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.
- **D.** Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Because the job always starts from `startingVersion = 0`, every execution replays the entire change feed and appends duplicates of previously processed rows.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html#read-change-data-feed



</details>

---

## Question 55

ADLT pipeline includes the following streaming tables:  + raw_iot ingests raw device measurement data from a heart rate tracking device.  + bpm_stats incrementally computes user statistics based on BPM measurements from raw_iot.  How can the data engineer configure this pipeline to be able to retain manually deleted or updated records in the raw_iot table, while recomputing the downstream table bpm_stats table when a pipeline update is run?

- **A.** Set the pipelines.reset.allowed property to false on raw_iot
- **B.** Set the skipChangeCommits flag to true on raw_iot
- **C.** Set the pipelines.reset.allowed property to false on bpm_stats
- **D.** Set the skipChangeCommits flag to true on bpm_stats

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Setting `pipelines.reset.allowed=false` on `raw_iot` prevents DLT from resetting or dropping the table, so manual edits persist while downstream tables can still be recomputed.

**Reference:** https://docs.databricks.com/en/delta-live-tables/settings.html#pipelinesresetallowed



</details>

---

## Question 56

A data pipeline uses Structured Streaming to ingest data from Apache Kafka to Delta Lake. Data is being stored in a bronze table, and includes the Kafka-generated timestamp, key, and value. Three months after the pipeline is deployed, the data engineering team has noticed some latency issues during certain times of the day.

The following helper is updated so every new batch records the Spark processing time as well as the Kafka topic and partition:

```python
from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.column import Column

def ingest_daily_batch(time_col: Column, year: int, month: int, day: int):
    (spark.read
        .format("parquet")
        .load(f"/mnt/daily_batch/{year}/{month}/{day}")
        .select(
            "*",
            time_col.alias("ingest_time"),
            input_file_name().alias("source_file")
        )
        .write
        .mode("append")
        .saveAsTable("bronze"))
```

The team plans to use these additional metadata fields to diagnose the transient processing delays.

Which limitation will the team face while diagnosing this problem?

- **A.** New fields will not be computed for historic records.
- **B.** Spark cannot capture the topic and partition fields from a Kafka source.
- **C.** Updating the table schema requires a default value provided for each field added.
- **D.** Updating the table schema will invalidate the Delta transaction log metadata.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Schema evolution only affects new writes; the newly added metadata columns will be populated for future Kafka records but historic rows will remain unchanged.

**Reference:** https://docs.databricks.com/en/delta/delta-schema-evolution.html#add-columns



</details>

---

## Question 57

A table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.  The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.  Which approach would simplify the identification of these changed records?

- **A.** Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
- **B.** Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
- **C.** Calculate the difference between the previous model predictions and the current customer_churn_params on a key identifying unique customers before making new predictions; only make predictions on those customers not in the previous predictions.
- **D.** Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being  written; use this field to identify records written on a particular date.
- **E.** Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Switching to a MERGE plus Delta’s change data feed lets the team identify only the rows that changed in the past 24 hours and predict on just those customers.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html#use-change-data-feed-to-build-cdc-pipelines



</details>

---

## Question 58

A nightly job ingests data into a Delta Lake table named `bronze`. The downstream step needs a helper that returns only the new records that have not yet been processed. Which snippet completes this function?

```
def new_records():
    ______
```

- **A.** `return spark.readStream.table("bronze")`
- **B.** `return spark.read.option("readChangeFeed", "true").table("bronze")`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Reading the Delta table with `readChangeFeed` returns only the new changes since the last read, so the helper function can emit just the not-yet-processed records.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html#read-change-data-feed



</details>

---

## Question 59

A junior data engineer is working to implement logic for a Lakehouse table named silver_device_recordings. The source data contains 100 unique fields in a highly nested JSON structure.  The silver_device_recordings table will be used downstream to power several production monitoring dashboards and a production model. At present, 45 of the 100 fields are being used in at least one of these applications.  The data engineer is trying to determine the best approach for dealing with schema declaration given the highly-nested structure of the data and the numerous fields.  Which of the following accurately presents information about Delta Lake and Databricks that may impact their decision-making process?

- **A.** The Tungsten ding used by D: icks is optimized for storing string data; newly-added native support for querying JSON strings means that string types are always most efficient.
- **B.** Because Delta Lake uses Parquet for data storage, data types can be easily evolved by just modifying file footer information in place.
- **C.** Schema inference and evolution on Databricks ensure that inferred types will always accurately match the data types used  by downstream systems.
- **D.** Because Databricks will infer schema using types that allow all observed data to be processed, setting types manually provides greater assurance of data quality enforcement.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Databricks infers permissive types that accommodate all observed values; specifying the schema manually enforces data quality and downstream compatibility.

**Reference:** https://docs.databricks.com/en/delta/delta-schema.html#specify-the-schema



</details>

---

## Question 60

The data engineering team maintains the following job:

```python
accountDF = spark.table("accounts")
orderDF   = spark.table("orders")
itemDF    = spark.table("items")

orderWithItemDF = (orderDF
    .join(itemDF, orderDF.itemID == itemDF.itemID)
    .select("accountID", "itemID", "orderID", "itemName"))

finalDF = (accountDF
    .join(orderWithItemDF, accountDF.accountID == orderWithItemDF.accountID)
    .select(orderWithItemDF["*"], accountDF.city))

(finalDF.write
        .mode("overwrite")
        .table("enriched_itemized_orders_by_account"))
```

Assuming the results are logically correct and the source tables are de-duplicated and validated, what happens when this job runs?

- **A.** A batch job will update the enriched_itemized_orders_by_account table, replacing only those rows that have different values than the current version of the table, using accountID as the primary key.
- **B.** The enriched_itemized_orders_by_account table will be overwritten using the current valid version of data in each of the three tables referenced in the join logic.
- **C.** No computation will occur until enriched_itemized_orders_by_account is queried; upon query materialization, results will be calculated using the current valid version of data in each of the three tables referenced in the join logic.
- **D.** An incremental job will detect if new rows have been written to any of the source tables; if new rows are detected, all results will be recalculated and used to overwrite the enriched_itemized_orders_by_account table.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Because `write.mode("overwrite")` is used after joining the three tables, the job recomputes the entire dataset and replaces `enriched_itemized_orders_by_account` with the new result set.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#overwrite-table



</details>

---

## Question 61

The data engineering team is configuring environments for development, testing, and production before beginning migration on a new data pipeline. The team requires extensive testing on both the code and data resulting from code execution, and the team wants to develop and test against data as similar to production data as possible.

A junior data engineer suggests that production data can be mounted to the development and testing environments, allowing pre-production code to execute against production data. Because all users have admin privileges in the development environment, the junior data engineer has offered to configure permissions and mount this data for the team.

Which statement captures best practices for this situation?

- **A.** All development, testing, and production code and data should exist in a single, unified workspace; creating separate environments for testing and development complicates administrative overhead.
- **B.** In environments where interactive code will be executed, production data should only be accessible with read permissions; creating isolated databases for each environment further reduces risks.
- **C.** Because access to production data will always be verified using passthrough credentials, it is safe to mount data to any Databricks development environment.
- **D.** Because Delta Lake versions all data and supports time travel, it is not possible for user error or malicious actors to permanently delete production data; as such, it is generally safe to mount production data anywhere.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Exposing production data to development clusters should be restricted to read-only access and preferably isolated copies per environment to reduce accidental writes or privilege escalation in sandboxes where everyone is an admin.

**Reference:** https://learn.microsoft.com/azure/databricks/security/security-best-practices

</details>

---

## Question 62

The data architect has mandated that all tables in the Lakehouse should be configured as external Delta Lake tables. Which approach will ensure that this requirement is met?

- **A.** Whenever a database is being created, make sure that the LOCATION keyword is used.
- **B.** When the workspace is being configured, make sure that external cloud object storage has been mounted.
- **C.** Whenever a table is being created, make sure that the LOCATION keyword is used.
- **D.** When tables are created, make sure that the UNMANAGED keyword is used in the CREATE TABLE statement.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Declaring `LOCATION` in each `CREATE TABLE` statement tells Delta to register the table against that external path, making it an unmanaged (external) table regardless of database defaults.

**Reference:** https://docs.databricks.com/en/tables/managed-and-unmanaged-tables.html

</details>

---

## Question 63

The marketing team is looking to share data in an aggregate table with the sales organization, but the field names used by the teams do not match, and a number of marketing-specific fields have not been approved for the sales org.

Which of the following solutions addresses the situation while emphasizing simplicity?

- **A.** Create a view on the marketing table selecting only those fields approved for the sales team; alias the names of any fields that should be standardized to the sales naming conventions.
- **B.** Create a new table with the required schema and use Delta Lake's DEEP CLONE functionality to sync up changes committed to one table to the corresponding table.
- **C.** Use a CTAS statement to create a derivative table from the marketing table; configure a production job to propagate changes.
- **D.** Add a parallel table write to the current production pipeline, updating a new sales table that varies as required from the marketing table.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** A simple view can expose only approved columns and rename them for the sales team without duplicating data or adding new ETL logic; the underlying marketing table remains the single source of truth.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html

</details>

---

## Question 64

A Delta Lake table representing metadata about content posts from users has the following schema:

```
user_id LONG, post_text STRING, post_id STRING,
longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE
```

This table is partitioned by the `date` column. A query is run with the following filter:

```
longitude < 20 AND longitude > -20
```

Which statement describes how data will be filtered?

- **A.** Statistics in the Delta log will be used to identify partitions that might include files in the filtered range.
- **B.** No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.
- **C.** The Delta Engine will scan the Parquet file footers to identify each row that meets the filter criteria.
- **D.** Statistics in the Delta log will be used to identify data files that might include records in the filtered range.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Even though the table is partitioned by date, Delta stores min/max statistics for other columns in the transaction log and uses those stats to skip Parquet files whose longitude range cannot satisfy the predicate.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html

</details>

---

## Question 65

A small company based in the United States has recently contracted a consulting firm in India to implement several new data engineering pipelines to power artificial intelligence applications. All the company's data is stored in regional cloud storage in the United States.

The workspace administrator at the company is uncertain about where the Databricks workspace used by the contractors should be deployed.

Assuming that all data governance considerations are accounted for, which statement accurately informs this decision?

- **A.** Databricks runs HDFS on cloud volume storage; as such, cloud virtual machines must be deployed in the region where the data is stored.
- **B.** Databricks workspaces do not rely on any regional infrastructure; as such, the decision should be made based upon what is most convenient for the workspace administrator.
- **C.** Cross-region reads and writes can incur significant costs and latency; whenever possible, compute should be deployed in the same region the data is stored.
- **D.** Databricks notebooks send all executable code from the user's browser to virtual machines over the open internet; whenever possible, choosing a workspace region near the end users is the most secure.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Provisioning the workspace in the same region as the backing storage keeps traffic local, minimizing egress fees and latency that otherwise accumulate when clusters read or write across regions.

**Reference:** https://learn.microsoft.com/azure/databricks/administration-guide/account-settings/azure-databricks-workspace-configuration#choose-workspace-region

</details>

---

## Question 66

A CHECK constraint has been successfully added to the Delta table named `activity_details` using the following logic:

```sql
ALTER TABLE activity_details
ADD CONSTRAINT valid_coordinates
CHECK (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180);
```

A batch job is attempting to insert new records to the table, including a record where `latitude = 45.50` and `longitude = 212.67`.

Which statement describes the outcome of this batch insert?

- **A.** The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log.
- **B.** The write will fail completely because of the constraint violation and no records will be inserted into the target table.
- **C.** The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table.
- **D.** The write will include all records in the target table; any violations will be indicated in the boolean column named `valid_coordinates`.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Delta constraints are enforced atomically; any batch containing a row outside the permitted ranges fails as a whole so that no invalid data is committed.

**Reference:** https://docs.databricks.com/en/delta/delta-constraints.html

</details>

---

## Question 67

A junior data engineer is migrating a workload from a relational database system to the Databricks Lakehouse. The source system uses a star schema, leveraging foreign key constraints and multi-table inserts to validate records on write.

Which consideration will impact the decisions made by the engineer while migrating this workload?

- **A.** Databricks only allows foreign key constraints on hashed identifiers, which avoid collisions in highly parallel writes.
- **B.** Foreign keys must reference a primary key field; multi-table inserts must leverage Delta Lake's upsert functionality.
- **C.** Committing to multiple tables simultaneously requires taking out multiple table locks and can lead to a state of deadlock.
- **D.** All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Delta enforces constraints such as `NOT NULL` and `CHECK` only within a single table; it does not provide multi-table transactions or foreign-key enforcement, so star schemas must encode those guarantees in the pipeline logic.

**Reference:** https://docs.databricks.com/en/delta/delta-constraints.html#limitations

</details>

---

## Question 68

A table is registered with the following code:

```sql
CREATE TABLE recent_orders AS
SELECT a.user_id, b.order_id, b.order_date
FROM users a
INNER JOIN (
  SELECT user_id, order_id, order_date
  FROM orders
  WHERE order_date >= current_date() - 7
) b ON a.user_id = b.user_id;
```

Both `users` and `orders` are Delta Lake tables. Which statement describes the results of querying `recent_orders`?

- **A.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- **B.** All logic will execute when the table is defined and store the result of joining tables to cloud storage; this stored data will be returned when the table is queried.
- **C.** Results will be computed and cached when the table is defined; these cached results will incrementally update as new records are inserted into source tables.
- **D.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.
- **E.** The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** `CREATE TABLE ... AS SELECT` materializes the query immediately and persists the resulting Delta table; future reads of `recent_orders` scan that stored data rather than re-running the joining logic.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using-select.html

</details>

---

## Question 69

A data architect has heard about Delta Lake's built-in versioning and time travel capabilities. For auditing purposes, they have a requirement to maintain a full record of all valid street addresses as they appear in the `customers` table.

The architect is interested in implementing a Type 1 table, overwriting existing records with new values and relying on Delta Lake time travel to support long-term auditing. A data engineer on the project feels that a Type 2 table will provide better performance and scalability.

Which piece of information is critical to this decision?

- **A.** Data corruption can occur if a query fails in a partially completed state because Type 2 tables require setting multiple fields in a single update.
- **B.** Shallow clones can be combined with Type 1 tables to accelerate historic queries for long-term versioning.
- **C.** Delta Lake time travel cannot be used to query previous versions of these tables because Type 1 changes modify data files in place.
- **D.** Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Time travel only retains files until they age past the retention interval (or are vacuumed), so relying on previous table versions for indefinite auditing can become costly or be removed entirely; modeling history explicitly (Type 2) scales better.

**Reference:** https://docs.databricks.com/en/delta/delta-time-travel.html#data-retention

</details>

---

## Question 70

A data engineer wants to join a stream of advertisement impressions (when an ad was shown) with another stream of user clicks on advertisements to correlate when impressions led to monetizable clicks.

In the code below, `impressions` is a streaming DataFrame with a watermark `withWatermark("event_time", "10 minutes")`:

```python
(clicks
    .groupBy(
        window("event_time", "5 minutes"),
        "id"
    )
    .count())

(impressions
    .withWatermark("event_time", "2 hours")
    .join(clicks, expr("clickAdId = impressionAdId"), "inner"))
```

The engineer notices the query slowing down significantly.

Which solution would improve the performance?

- **A.** Join on the constraint `clickTime >= impressionTime AND clickTime <= impressionTime + interval 1 hour` so the event-time range is bounded.
- **B.** Join on the constraint `clickTime + 3 hours < impressionTime - 2 hours`.
- **C.** Join on the constraint `clickTime == impressionTime` using a `leftOuter` join.
- **D.** Join on the constraint `clickTime >= impressionTime - interval 3 hours` and remove all watermarks.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Stream-stream joins need both sides to define a finite event-time range so the engine can drop old state; bounding the click time relative to impression time lets the watermark clear stale keys and keeps state manageable.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins

</details>

---

## Question 71

A junior data engineer has manually configured a series of jobs using the Databricks Jobs UI. Upon reviewing their work, the engineer realizes that they are listed as the "Owner" for each job. They attempt to transfer "Owner" privileges to the `DevOps` group, but cannot successfully accomplish this task.

Which statement explains what is preventing this privilege transfer?

- **A.** Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group.
- **B.** The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed.
- **C.** Only workspace administrators can grant "Owner" privileges to a group.
- **D.** A user can only transfer job ownership to a group if they are also a member of that group.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Each job has a single owner that must be an individual user; groups can be granted `Can Manage` permissions, but ownership cannot be assigned to a group principal.

**Reference:** https://learn.microsoft.com/azure/databricks/workflows/jobs/jobs#job-ownership

</details>

---

## Question 72

A table named `user_ltv` is being used to create a view that will be used by data analysts on various teams. Users in the workspace are configured into groups, which are used for setting up data access using ACLs. The `user_ltv` table has the following schema: `email STRING, age INT, ltv INT`.

The following view definition is executed:

```sql
CREATE VIEW user_ltv_no_minors AS
SELECT email, age, ltv
FROM user_ltv
WHERE CASE WHEN is_member('auditing') THEN TRUE ELSE age >= 18 END;
```

An analyst who is not a member of the `auditing` group executes the following query:

```sql
SELECT * FROM user_ltv_no_minors;
```

Which statement describes the results returned by this query?

- **A.** All columns will be displayed normally for those records that have an age greater than 17; records not meeting this condition will be omitted.
- **B.** All age values less than 18 will be returned as null values, all other columns will be returned with the values in `user_ltv`.
- **C.** All values for the age column will be returned as null values, all other columns will be returned with the values in `user_ltv`.
- **D.** All columns will be displayed normally for those records that have an age greater than 18; records not meeting this condition will be omitted.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** For users outside the auditing group, the CASE expression evaluates to `age >= 18`, so only records with `age` 18 or greater are returned and the columns remain unchanged.

**Reference:** https://docs.databricks.com/en/sql/language-manual/functions/is_member.html

</details>

---

## Question 73

All records from an Apache Kafka producer are being ingested into a single Delta Lake table with the following schema:

```
key BINARY, value BINARY, topic STRING,
partition LONG, offset LONG, timestamp LONG
```

There are five unique topics being ingested. Only the `registration` topic contains Personally Identifiable Information (PII). The company wishes to restrict access to PII and to retain those records only for 14 days after initial ingestion. Non-PII records should be retained indefinitely.

Which solution meets the requirements?

- **A.** All data should be deleted biweekly; Delta Lake's time travel functionality should be leveraged to maintain a history of non-PII information.
- **B.** Data should be partitioned by the `registration` field, allowing ACLs and delete statements to be set for the PII directory.
- **C.** Data should be partitioned by the `topic` field, allowing ACLs and delete statements to leverage partition boundaries.
- **D.** Separate object storage containers should be specified based on the partition field, allowing isolation at the storage level.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Partitioning by `topic` lets security policies and retention logic target the `topic = 'registration'` partitions specifically, enabling ACL isolation and partition-pruned deletes without touching the other four topics.

**Reference:** https://docs.databricks.com/en/delta/delta-partitioning.html

</details>

---

## Question 74

The data governance team is reviewing code used for deleting records for compliance with GDPR. The following logic has been implemented to propagate delete requests from the `user_lookup` table to the `user_aggregates` table.

```python
(spark.read
    .format("delta")
    .option("readChangeData", True)
    .option("startingTimestamp", "2021-08-22 00:00:00")
    .option("endingTimestamp", "2021-08-29 00:00:00")
    .table("user_lookup")
    .createOrReplaceTempView("changes"))

spark.sql("""
DELETE FROM user_aggregates
WHERE user_id IN (
  SELECT user_id
  FROM changes
  WHERE _change_type = 'delete'
)
""")
```

Assuming that `user_id` is a unique identifying key and that all users that have requested deletion have been removed from the `user_lookup` table, which statement describes whether successfully executing the above logic guarantees that the records to be deleted from the `user_aggregates` table are no longer accessible, and why?

- **A.** No; the Delta Lake DELETE command only provides ACID guarantees when combined with the MERGE INTO command.
- **B.** No; files containing deleted records may still be accessible with time travel until a VACUUM command is used to remove invalidated data files.
- **C.** No; the change data feed only tracks inserts and updates, not deleted records.
- **D.** Yes; Delta Lake ACID guarantees provide assurance that the DELETE command succeeded fully and permanently purged these records.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Deletes immediately hide rows from the latest table version, but the underlying data files remain until they expire past the retention period or are vacuumed, so time travel could still reveal the records.

**Reference:** https://docs.databricks.com/en/delta/delta-retention.html

</details>

---

## Question 75

An external object storage container has been mounted to the location `/mnt/finance_eda_bucket`. The following logic was executed to create a database for the finance team:

```sql
CREATE DATABASE finance_eda_db
LOCATION '/mnt/finance_eda_bucket';

GRANT USAGE ON DATABASE finance_eda_db TO finance;
GRANT CREATE ON DATABASE finance_eda_db TO finance;
```

After the database was successfully created and permissions configured, a member of the finance team runs the following code:

```sql
CREATE TABLE finance_eda_db.tx_sales AS
SELECT *
FROM sales
WHERE state = 'TX';
```

If all users on the finance team are members of the `finance` group, which statement describes how the `tx_sales` table will be created?

- **A.** A logical table will persist the query plan to the Hive metastore in the Databricks control plane.
- **B.** An external table will be created in the storage container mounted to `/mnt/finance_eda_bucket`.
- **C.** A managed table will be created in the DBFS root storage container.
- **D.** A managed table will be created in the storage container mounted to `/mnt/finance_eda_bucket`.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Because the database itself specifies a `LOCATION`, new managed tables inherit that path, so the CTAS statement creates a managed Delta table whose files live under `/mnt/finance_eda_bucket`.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-database.html

</details>

---

## Question 76

The data engineering team has been tasked with configuring connections to an external database that does not have a supported native connector with Databricks. The external database already has data security configured by group membership. These groups map directly to user groups already created in Databricks that represent various teams within the company.

A new login credential has been created for each group in the external database. The Databricks Utilities Secrets module will be used to make these credentials available to Databricks users.

Assuming that all the credentials are configured correctly on the external database and group membership is properly configured on Databricks, which statement describes how teams can be granted the minimum necessary access to using these credentials?

- **A.** No additional configuration is necessary as long as all users are configured as administrators in the workspace where secrets have been added.
- **B.** "Read" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
- **C.** "Read" permissions should be set on a secret scope containing only those credentials that will be used by a given team.
- **D.** "Manage" permissions should be set on a secret scope containing only those credentials that will be used by a given team.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Access is controlled at the secret-scope level; granting a team read permissions on the scope that stores its credentials lets notebooks retrieve the secrets without giving unnecessary manage rights.

**Reference:** https://docs.databricks.com/en/security/secrets/secrets.html#manage-access

</details>

---

## Question 77

What is the retention of job run history?

- **A.** It is retained until you export or delete job run logs.
- **B.** It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3.
- **C.** It is retained for 60 days, during which you can export notebook run results to HTML.
- **D.** It is retained for 60 days, after which logs are archived.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Databricks keeps job run details and notebook results for 60 days, and during that time runs can be downloaded or exported via the Jobs UI or API.

**Reference:** https://learn.microsoft.com/azure/databricks/workflows/jobs/jobs#job-run-details

</details>

---

## Question 78

A data engineer, User A, has promoted a new pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.

Which statement describes the contents of the workspace audit logs concerning these events?

- **A.** Because the REST API was used for job creation and triggering runs, a service principal will be automatically used to identify these events.
- **B.** Because User A created the jobs, their identity will be associated with both the job creation events and the job run events.
- **C.** Because these events are managed separately, User A will have their identity associated with the job creation events and User B will have their identity associated with the job run events.
- **D.** Because the REST API was used for job creation and triggering runs, user identity will not be captured in the audit logs.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Audit logs capture the identity tied to each personal access token invocation, so creating the job records User A's identity, while triggering runs via User B's token records User B for those run events.

**Reference:** https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html

</details>

---

## Question 79

A production workload incrementally applies updates from an external Change Data Capture feed to a Delta Lake table as an always-on Structured Streaming job. When data was initially migrated for this table, `OPTIMIZE` was executed and most data files were resized to 1 GB. Auto Optimize and Auto Compaction were both turned on for the streaming production job. Recent review of data files shows that most data files are under 64 MB, although each partition in the table contains at least 1 GB of data and the total table size is over 10 TB.

Which of the following likely explains these smaller file sizes?

- **A.** Databricks has autotuned to a smaller target file size to reduce duration of MERGE operations.
- **B.** Z-order indices calculated on the table are preventing file compaction.
- **C.** Bloom filter indices calculated on the table are preventing file compaction.
- **D.** Databricks has autotuned to a smaller target file size based on the overall size of data in the table.
- **E.** Databricks has autotuned to a smaller target file size based on the amount of data in each partition.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Auto Optimize targets compaction to partition-level heuristics; when micro-batches write relatively little per partition, the service creates smaller files (often ~64 MB) per partition even if the table overall is large.

**Reference:** https://docs.databricks.com/en/delta/delta-optimizations.html#auto-optimize

</details>

---

## Question 80

A distributed team of data analysts share computing resources on an interactive cluster with autoscaling configured. To better manage costs and query throughput, the workspace administrator wants to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries.

Where can the administrator review the timeline for cluster resizing events?

- **A.** Workspace audit logs
- **B.** Driver's log file
- **C.** Ganglia
- **D.** Cluster Event Log

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The Cluster Event Log captures autoscaling resize events chronologically, including timestamps and reasons for scaling, which helps analyze what triggered worker changes.

**Reference:** https://docs.databricks.com/en/clusters/clusters-manage.html#cluster-event-log

</details>

---

## Question 81

When evaluating the Ganglia Metrics for a given cluster with three executor nodes, which indicator would signal proper utilization of the VM's resources?

- **A.** The five-minute load average remains consistent/flat
- **B.** CPU utilization is around 75%
- **C.** Network I/O never spikes
- **D.** Total disk space remains constant

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Ganglia should show CPUs working most of the time—roughly 70–80% utilization indicates the executors are well used without being saturated.

**Reference:** https://docs.databricks.com/en/clusters/clusters-monitor.html#view-cluster-metrics-in-ganglia

</details>

---

## Question 82

The data engineer is using Spark's `MEMORY_ONLY` storage level. Which indicators should the engineer look for in the Spark UI Storage tab to signal that a cached table is not performing optimally?

- **A.** On-heap memory usage is within 75% of off-heap memory usage
- **B.** The RDD block name includes the "*" annotation, signaling a failure to cache
- **C.** Size on disk is > 0
- **D.** The number of cached partitions > the number of Spark partitions

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The Storage tab marks uncached partitions with an asterisk; seeing `*` beside an RDD block indicates MEMORY_ONLY could not hold that partition in memory.

**Reference:** https://spark.apache.org/docs/latest/monitoring.html#storage-tab

</details>

---

## Question 83

Review the following error traceback:

```text
AnalysisException                         Traceback (most recent call last)
<command-3293767849433498> in <module>
----> 1 display(df.select(3*"heartrate"))

/databricks/spark/python/pyspark/sql/dataframe.py in select(self, *cols)
   1690 [Row(name="Alice', age=12), Row(name='Bob', age=15)]
-> 1692 jdf = self._jdf.select(self._jcols(*cols))
   1693 return DataFrame(jdf, self.sql_ctx)

/databricks/spark/python/lib/py4j-0.10.9-src.zip/py4j/java_gateway.py in _call__(self, *args)
   1303 answer = self.gateway_client.send_command(command)
-> 1304 return_value = get_return_value(
   1305     answer, self.gateway_client, self.target_id, self.name)

/databricks/spark/python/pyspark/sql/utils.py in deco(*a, **kw)
    121 # Hide where the exception came from that shows a non-Pythonic
--> 123 raise converted from None
    124 else:
    125     raise

AnalysisException: cannot resolve `heartrateheartrateheartrate` given input columns:
[spark_catalog.database.table.device_id,
 spark_catalog.database.table.heartrate,
 spark_catalog.database.table.mrn,
 spark_catalog.database.table.time]
```

Which statement describes the error being raised?

- **A.** There is a syntax error because the heartrate column is not correctly identified as a column
- **B.** There is no column in the table named `heartrateheartrateheartrate`
- **C.** There is a type error because a column object cannot be multiplied
- **D.** There is a type error because a DataFrame object cannot be multiplied

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Spark raises an `AnalysisException` when a referenced column name does not exist in the input schema; the expression refers to `heartrateheartrateheartrate`, which is absent.

**Reference:** https://spark.apache.org/docs/latest/sql-programming-guide.html#running-sql-queries-programmatically

</details>

---

## Question 84

What is a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

- **A.** Run `source env/bin/activate` in a notebook setup script
- **B.** Install libraries from PyPI using the cluster UI
- **C.** Use `%pip install` in a notebook cell
- **D.** Use `%sh pip install` in a notebook cell

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `%pip install` scopes package installation to the current notebook session and propagates the environment to every node attached to the cluster.

**Reference:** https://docs.databricks.com/en/libraries/notebooks-python-libraries.html#install-python-packages

</details>

---

## Question 85

What is the first line of a Databricks Python notebook when viewed in a text editor?

- **A.** `%python`
- **B.** `// Databricks notebook source`
- **C.** `# Databricks notebook source`
- **D.** `-- Databricks notebook source`

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Exporting a Databricks notebook as source prepends `# Databricks notebook source` to indicate the file format.

**Reference:** https://docs.databricks.com/en/notebooks/notebook-export.html

</details>

---

## Question 86

Incorporating unit tests into a PySpark application requires upfront attention to job design or refactoring of existing code. Which benefit offsets this additional effort?

- **A.** Improves the quality of your data
- **B.** Validates a complete use case of your application
- **C.** Troubleshooting is easier since all steps are isolated and tested individually
- **D.** Ensures that all steps interact correctly to achieve the desired end result

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Unit tests isolate components, so failures point to a specific function or transformation, dramatically simplifying troubleshooting.

**Reference:** https://martinfowler.com/articles/practical-test-pyramid.html#UnitTests

</details>

---

## Question 87

What describes integration testing?

- **A.** It validates an application use case
- **B.** It validates behavior of individual elements of an application
- **C.** It requires an automated testing framework
- **D.** It validates interactions between subsystems of your application

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Integration tests focus on how multiple subsystems collaborate—verifying their interfaces and data flow rather than isolated behavior.

**Reference:** https://martinfowler.com/articles/practical-test-pyramid.html#IntegrationTests

</details>

---

## Question 88

The Databricks CLI is used to trigger a run of an existing job by passing the `job_id` parameter. The response includes a field `run_id`. What does the number alongside this field represent?

- **A.** The `job_id` and number of times the job has run are concatenated
- **B.** The globally unique ID of the newly triggered run
- **C.** The number of times the job definition has been run in this workspace
- **D.** The `job_id`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The Jobs API returns a unique `run_id` for every invocation so you can query run status or results later.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#create-a-run-now

</details>

---

## Question 89

A Databricks job has three notebook tasks: Task A runs first, Tasks B and C depend on A and then run in parallel. If Tasks A and B succeed but Task C fails, what is the resulting state?

- **A.** Tasks A and B finish successfully; Task C may have partial work before failing
- **B.** Unless all tasks succeed, no changes are committed
- **C.** The dependency graph rolls back all work until every task succeeds
- **D.** Tasks A and B finish, and any changes from Task C are automatically rolled back

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Each task executes its notebook independently; successful tasks keep their effects even if a later parallel task fails.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs.html#tasks-and-dependencies

</details>

---

## Question 90

Which statement regarding stream-static joins and static Delta tables is correct?

- **A.** Each micro-batch uses the most recent version of the static table at execution time
- **B.** Each micro-batch uses the version of the static table that was current when the streaming query started
- **C.** The checkpoint directory stores state information for distinct keys in the static table
- **D.** Stream-static joins cannot read static Delta tables because of consistency issues
- **E.** The checkpoint directory tracks updates to the static Delta table

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Structured Streaming snapshots the static table when the query starts and reuses that version across every subsequent micro-batch for consistency.

**Reference:** https://docs.databricks.com/en/structured-streaming/stream-static-joins.html

</details>

---

## Question 91

When scheduling Structured Streaming jobs for production, which Databricks Job configuration automatically recovers from query failures while keeping costs low?

- **A.** Cluster: new job cluster; Retries: unlimited; Maximum concurrent runs: 1
- **B.** Cluster: new job cluster; Retries: unlimited; Maximum concurrent runs: unlimited
- **C.** Cluster: existing all-purpose cluster; Retries: unlimited; Maximum concurrent runs: 1
- **D.** Cluster: new job cluster; Retries: none; Maximum concurrent runs: 1

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Running a streaming job on a dedicated job cluster with unlimited retries ensures automatic recovery while shutting the cluster down when idle to control cost.

**Reference:** https://docs.databricks.com/en/structured-streaming/deploy.html#schedule-structured-streaming-jobs

</details>

---

## Question 92

A Delta Lake table was created with `CREATE TABLE prod.sales_by_stor USING DELTA LOCATION '/mnt/prod/sales_by_store'`. After noticing the typo, the following command was run:

```sql
ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_store;
```

What happens after running the second command?

- **A.** The table reference in the metastore is updated
- **B.** All related files and metadata are dropped and recreated
- **C.** The table name change is recorded in the Delta transaction log
- **D.** A new Delta transaction log is created

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `ALTER TABLE ... RENAME TO` changes only the metastore entry; the data files and transaction log remain untouched at the original location.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-rename.html

</details>

---

## Question 93

A Databricks SQL alert runs the following query every minute:

```sql
SELECT mean(temperature), max(temperature), min(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id;
```

The alert triggers when `mean(temperature) > 120` and sends notifications at most once per minute. If notifications fire for three consecutive minutes and then stop, what must be true?

- **A.** The total average temperature across all sensors exceeded 120 for three runs
- **B.** At least one sensor's average temperature exceeded 120 for three consecutive runs
- **C.** The query failed to update for three minutes and then restarted
- **D.** The maximum temperature for a sensor exceeded 120 for three runs

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Alerts evaluate each grouped row independently; repeated notifications mean the condition held for at least one `sensor_id` in three successive executions.

**Reference:** https://docs.databricks.com/en/sql/user/alerts.html#threshold-alerts

</details>

---

## Question 94

Users need to start and attach to an existing interactive cluster. Assuming they currently have no permissions, what is the minimal privilege required?

- **A.** Can Manage on the cluster
- **B.** Cluster creation allowed plus Can Restart on the cluster
- **C.** Cluster creation allowed plus Can Attach To on the cluster
- **D.** Can Restart on the cluster

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The `Can Restart` permission includes the ability to start, restart, and attach notebooks to the cluster without granting full managerial control.

**Reference:** https://docs.databricks.com/en/clusters/permissions.html#cluster-permissions

</details>

---

## Question 95

The data science team logged a production MLflow model. The following code produces a DataFrame `preds` with columns `customer_id`, `predictions`, and the current `date`:

```python
from pyspark.sql.functions import current_date

model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time_since_last_seen", "app_rating"]

preds = (df.select("customer_id", model(*columns).alias("predictions"))
           .withColumn("date", current_date()))
```

Predictions are generated once per day and should be comparable across days. Which action completes the task while minimizing compute cost?

- **A.** `preds.write.mode("append").saveAsTable("churn_preds")`
- **B.** `preds.write.format("delta").save("/preds/churn_preds")`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Appending to a managed Delta table preserves every day’s predictions without rewriting historic data, enabling time-based comparisons at minimal cost.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#write-to-a-table

</details>

---

## Question 96

A Delta table of weather records (date, device_id, temp, latitude, longitude) is partitioned by date. To filter for `latitude > 66.3`, how does Delta Lake identify which files to load?

- **A.** Cache all records to an operational database
- **B.** Scan Parquet file footers for latitude statistics
- **C.** Scan the Hive metastore for latitude statistics
- **D.** Scan the Delta log for min/max latitude statistics

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Delta stores per-file statistics, including column min/max values, in the transaction log and uses them for data skipping before reading Parquet files.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html

</details>

---

## Question 97

A streaming DataFrame `df` has schema `device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT`. To compute average humidity and temperature for each non-overlapping five-minute interval, fill in the missing expression:

```python
(df.withWatermark("event_time", "10 minutes")
   .groupBy(
       ______,
       "device_id"
   )
   .agg(
       avg("temp").alias("avg_temp"),
       avg("humidity").alias("avg_humidity")
   )
   .writeStream
   .format("delta")
   .saveAsTable("sensor_avg"))
```

- **A.** `to_interval("event_time", "5 minutes").alias("time")`
- **B.** `window("event_time", "5 minutes").alias("time")`
- **C.** `"event_time"`
- **D.** `lag("event_time", "10 minutes").alias("time")`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Structured Streaming groups time-based aggregates via the `window` function, which slices event time into fixed five-minute buckets.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#aggregate-operations

</details>

---

## Question 98

Development tables are created with `SHALLOW CLONE`. After a source table is vacuumed, the cloned Type 1 SCD tables stop working. Why?

- **A.** Type 1 tables cannot be cloned
- **B.** Running VACUUM invalidates all shallow clones; deep clones must be used
- **C.** The compacted files are not tracked by the cloned metadata and refreshing pulls them in
- **D.** Shallow clones reference the source data files, so VACUUM deleted files that the clone still needed

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** A shallow clone references the source table’s data files; if VACUUM purges those files, the clone’s metadata points to missing data and queries fail.

**Reference:** https://docs.databricks.com/en/delta/delta-manage.html#clone-a-table

</details>

---

## Question 99

A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint `2.0/jobs/create`:

```json
{
  "name": "Ingest new data",
  "existing_cluster_id": "6015-954420-peace720",
  "notebook_task": {
    "notebook_path": "/Prod/ingest.py"
  }
}
```

Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

- **A.** The notebook logic runs three times on the existing all-purpose cluster
- **B.** The notebook logic runs three times on new clusters cloned from the provided cluster ID
- **C.** Three new job definitions named "Ingest new data" are created, but none are executed
- **D.** One new job definition is created but not executed

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `jobs/create` only registers job definitions; each request creates a new job record but does not trigger any runs.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#create

</details>

---

## Question 100

A Delta Lake table in the Lakehouse named `customer_churn_params` is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.

Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.

Given the current implementation, which method can be used?

- **A.** Execute a query to calculate the difference between the new version and the previous version using Delta Lake’s built-in versioning and lime travel functionality.
- **B.** Parse the Delta Lake transaction log to identify all newly written data files.
- **C.** Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.
- **D.** Execute `DESCRIBE HISTORY customer_churn_params` to obtain the full operation metrics for the update, including a log of all records that have been added or modified.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Delta’s time travel lets you query any previous version by timestamp or version number, so you can diff the latest version against the prior one right after an overwrite.

**Reference:** https://docs.databricks.com/en/delta/delta-time-travel.html

</details>

---

## Question 101

A view is registered with the following code:

```sql
CREATE VIEW recent_orders AS
SELECT a.user_id, a.email, b.order_id, b.order_date
FROM users a
INNER JOIN (
  SELECT user_id, order_id, order_date
  FROM orders
  WHERE order_date >= current_date() - 7
) b ON a.user_id = b.user_id;
```

Both `users` and `orders` are Delta Lake tables. Which statement describes the results of querying `recent_orders`?

- **A.** The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.
- **B.** All logic will execute when the table is defined and store the result of joining tables to DBFS; this stored data will be returned when the view is queried.
- **C.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
- **D.** All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** A view simply stores the query logic; every time `recent_orders` is read it re-evaluates the join against the current versions of `users` and `orders`.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html

</details>

---

## Question 102

A data engineer is performing a join operation to combine values from a static `userLookup` table with a streaming DataFrame `streamingDF`. Which code block attempts to perform an invalid stream-static join?

- **A.** `userLookup.join(streamingDF, ["user_id"], how="right")`
- **B.** `streamingDF.join(userLookup, ["user_id"], how="inner")`
- **C.** `userLookup.join(streamingDF, ["user_id"], how="inner")`
- **D.** `userLookup.join(streamingDF, ["user_id"], how="left")`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Stream-static joins must call `join` on the streaming DataFrame, and only `inner`, `leftOuter`, `rightOuter` (depending on which side is streaming), and `leftSemi` joins are supported. Joining from the static table with a `right` join attempts to treat the static table as streaming, which is invalid.

**Reference:** https://docs.databricks.com/en/structured-streaming/stream-static-joins.html

</details>

---

## Question 103

A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame `df`. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval while maintaining incremental state for 10 minutes. Complete the missing call in the code block:

```python
(df ______
   .groupBy(
       window("event_time", "5 minutes").alias("time"),
       "device_id"
   )
   .agg(
       avg("temp").alias("avg_temp"),
       avg("humidity").alias("avg_humidity")
   )
   .writeStream
   .format("delta")
   .saveAsTable("sensor_avg"))
```

Which response correctly fills the blank?

- **A.** `withWatermark("event_time", "10 minutes")`
- **B.** `awaitArrival("event_time", "10 minutes")`
- **C.** `await("event_time" + "10 minutes")`
- **D.** `slidingWindow("event_time", "10 minutes")`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** To retain state for only 10 minutes, the stream must call `withWatermark(...)`; the rest of the code already defines the five-minute windows.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking

</details>

---

## Question 104

Two Structured Streaming jobs will concurrently write to the same bronze Delta table. Each job subscribes to a different Kafka topic but they share schema. The engineer proposes to share a single nested checkpoint directory for both streams as shown:

```
/bronze
  |_ _checkpoint
       |_ _delta_log
       |_ year_week=2020_01
       |_ year_week=2020_02
```

Is this checkpoint directory structure valid?

- **A.** No; Delta Lake streaming checkpoints live only in the transaction log.
- **B.** Yes; both streams can share a single checkpoint directory.
- **C.** No; only one stream can write to a Delta Lake table.
- **D.** No; each stream needs its own checkpoint directory.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Structured Streaming requires a dedicated checkpoint directory per query; sharing checkpoints corrupts progress and offsets for both streams.

**Reference:** https://docs.databricks.com/en/structured-streaming/production.html#checkpoint-location

</details>

---

## Question 105

A Structured Streaming job is configured with a 10-second trigger interval, but peak-hour batches occasionally take 30 seconds. Records must be processed in under 10 seconds. Which adjustment meets the requirement?

- **A.** Decrease the trigger interval to 5 seconds so idle executors can work on the next batch while long tasks finish.
- **B.** Decrease the trigger interval to 5 seconds to avoid large micro-batches building up and spilling.
- **C.** Leave the trigger interval unchanged and increase shuffle partitions.
- **D.** Use `trigger(once=True)` every 10 seconds.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Smaller trigger intervals reduce batch size and backpressure, helping batches consistently finish under 10 seconds without waiting for large spikes.

**Reference:** https://docs.databricks.com/en/structured-streaming/triggers.html

</details>

---

## Question 106

Which statement describes the default execution mode for Databricks Auto Loader?

- **A.** Cloud queue notifications feed new files incrementally into Delta tables.
- **B.** Directory listing identifies new files and the table is created by querying all existing files.
- **C.** Webhooks fire whenever new data arrives and Auto Loader merges it automatically.
- **D.** Auto Loader lists the input path to detect new files and incrementally, idempotently loads them into the target Delta table.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** In its default directory-listing mode, Auto Loader tracks discovered files to ensure each is loaded exactly once into the target table.

**Reference:** https://docs.databricks.com/en/ingestion/auto-loader/index.html#directory-listing-mode

</details>

---

## Question 107

Which statement describes the correct use of `pyspark.sql.functions.broadcast`?

- **A.** It marks a column as low-cardinality to map values to partitions.
- **B.** It marks a column as small enough to broadcast.
- **C.** It caches a table on all nodes for reuse across jobs.
- **D.** It marks an entire DataFrame as small enough to send to every executor to enable a broadcast join.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `broadcast(df)` wraps a DataFrame so Spark materializes it in driver memory and ships it to each executor, enabling efficient broadcast hash joins.

**Reference:** https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.broadcast.html

</details>

---

## Question 108

Users must start and attach to existing interactive clusters. Assuming they currently have no permissions, what minimal privilege is required?

- **A.** Can Manage
- **B.** Workspace Admin plus Can Attach To
- **C.** Cluster creation allowed plus Can Attach To
- **D.** Can Restart
- **E.** Cluster creation allowed plus Can Restart

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `Can Restart` permits a user to start or restart an existing cluster and attach notebooks without granting broader management permissions.

**Reference:** https://docs.databricks.com/en/clusters/permissions.html#cluster-permissions

</details>

---

## Question 109

Two Structured Streaming jobs both write to the same Delta table but subscribe to different Kafka topics. The engineer proposes this shared checkpoint layout:

```
/bronze
  |_ _checkpoint
  |_ _delta_log
  |_ year_week=2020_01
  |_ year_week=2020_02
```

Is this valid?

- **A.** No; Delta Lake manages checkpoints in the transaction log.
- **B.** Yes; both streams can share one checkpoint.
- **C.** No; only one stream can write to the table.
- **D.** Yes; Delta supports infinite writers.
- **E.** No; each stream must have its own checkpoint.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Each Structured Streaming query needs its own isolated checkpoint directory to track offsets and state; sharing a directory corrupts both streams.

**Reference:** https://docs.databricks.com/en/structured-streaming/production.html#checkpoint-location

</details>

---

## Question 110

Where in the Spark UI can you detect partitions spilling to disk?

- **A.** Stage Detail screen and Query Detail screen
- **B.** Stage Detail screen and Executor log files
- **C.** Driver log file and Executor log files
- **D.** Executor Detail screen and Executor log files

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The Stage Detail page reveals spill metrics in the task table, while executor logs provide spill warnings for the affected tasks.

**Reference:** https://docs.databricks.com/en/jobs/monitor-run.html#use-the-spark-ui

</details>

---

## Question 111

A nightly batch job ingests the previous day’s Parquet files and deduplicates by `customer_id` and `order_id` before saving to a Delta table:

```python
(spark.read
      .format("parquet")
      .load(f"/mnt/raw_orders/{date}")
      .dropDuplicates(["customer_id", "order_id"])
      .write
      .mode("append")
      .saveAsTable("orders"))
```

If the upstream system occasionally emits duplicates hours apart, which statement is correct?

- **A.** Only unique records are written and duplicates already in the table are removed.
- **B.** Each batch writes unique records, but duplicates that already exist in `orders` remain because dropDuplicates only affects the incoming batch.
- **C.** Existing duplicates in the target table are overwritten automatically.
- **D.** Drop duplicates runs across both existing and new data, ensuring no duplicates remain.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** `dropDuplicates` operates on the DataFrame being written, so duplicates separated across batches remain in the table.

**Reference:** https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dropDuplicates.html

</details>

---

## Question 112

Given the following MERGE:

```sql
MERGE INTO events
USING new_events
ON events.event_id = new_events.event_id
WHEN NOT MATCHED THEN INSERT *;
```

What happens to new records whose `event_id` already exists in `events`?

- **A.** They are merged.
- **B.** They are ignored.
- **C.** They are updated.
- **D.** They are inserted.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** With only a `WHEN NOT MATCHED` clause, rows whose key matches an existing record are skipped—they are neither updated nor inserted.

**Reference:** https://docs.databricks.com/en/delta/merge.html

</details>

---

## Question 113

A critical Kafka field was omitted in the ingestion logic, so it never arrived in Delta or downstream storage. Kafka retains data for seven days, but the pipeline has run for months. How can Delta Lake prevent this type of data loss in the future?

- **A.** The Delta log stores the full history of the Kafka producer.
- **B.** Schema evolution can retroactively calculate missing values.
- **C.** Delta automatically validates that all source fields are ingested.
- **D.** Landing raw Kafka data into a bronze Delta table preserves every field and provides a replayable history even if later processing omits a column.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Persisting the raw feed in a bronze table ensures all source columns are retained and can be replayed if downstream schemas change.

**Reference:** https://docs.databricks.com/en/delta/bronze-silver-gold.html

</details>

---

## Question 114

The following batch job aggregates `silver_customer_sales` and overwrites `gold_customer_lifetime_sales_summary`:

```python
import pyspark.sql.functions as F

(spark.table("silver_customer_sales")
  .groupBy("customer_id")
  .agg(F.min("sale_date").alias("first_transaction_date"),
       F.max("sale_date").alias("last_transaction_date"),
       F.mean("sale_total").alias("average_sales"),
       F.countDistinct("order_id").alias("total_orders"),
       F.sum("sale_total").alias("lifetime_value"))
  .write
  .mode("overwrite")
  .table("gold_customer_lifetime_sales_summary"))
```

Assuming the source table is validated, what happens when this job runs?

- **A.** The silver table is overwritten by aggregated values.
- **B.** Only changed rows in the gold table are replaced.
- **C.** The gold table is fully overwritten with the newly computed aggregates.
- **D.** An incremental job updates only if new rows were added to `silver_customer_sales`.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Because `.write.mode("overwrite")` is used, the job recomputes all aggregates and replaces the entire gold table each run.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#overwrite-table

</details>

---

## Question 115

An organization is migrating thousands of objects into bronze, silver, and gold layers. Bronze holds engineering workloads; silver serves engineering and ML; gold supports BI. PII exists everywhere but is anonymized at silver and gold. What best practice balances security with collaboration?

- **A.** Isolate tables in separate databases per quality tier to simplify ACLs and storage layouts.
- **B.** Database organization has no impact on security.
- **C.** Store everything in one database and grant view rights to all users.
- **D.** Use the default Databricks database for managed-table security.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Segregating data-quality tiers into separate schemas makes it easier to apply appropriate ACLs and storage locations for managed tables.

**Reference:** https://learn.microsoft.com/azure/databricks/lakehouse/medallion-architecture

</details>

---

## Question 116

To ensure every table is external/unmanaged, what approach works?

- **A.** Specify `LOCATION` when creating each database.
- **B.** Mount external storage when configuring the workspace.
- **C.** When saving data to a table, specify the path with `USING DELTA` so the table registers as external.
- **D.** Use the `UNMANAGED` keyword in `CREATE TABLE`.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Providing an explicit path in the `CREATE TABLE ... USING DELTA LOCATION` command registers the table as unmanaged (external) regardless of database defaults.

**Reference:** https://docs.databricks.com/en/tables/managed-and-unmanaged-tables.html

</details>

---

## Question 117

An aggregate table must add and rename several columns to satisfy a customer-facing app, but other teams rely on its current schema. How can the team minimize disruption while avoiding extra tables?

- **A.** Notify users to adjust their queries.
- **B.** Create a new table for the app and a view with aliases to preserve the old schema.
- **C.** Create a new table and use deep clone to keep schemas in sync.
- **D.** Replace the table with a logical view and build a new table for the app.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Writing the new schema to a fresh table and exposing a compatibility view lets existing consumers keep their schema while the app uses the new fields.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html

</details>

---

## Question 118

Given a table with columns `user_id`, `post_text`, `post_id`, `longitude`, `latitude`, `post_time`, `date`, which column is best suited for partitioning?

- **A.** `post_time`
- **B.** `date`
- **C.** `post_id`
- **D.** `user_id`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** `date` has manageable cardinality and aligns with typical filter predicates, whereas `post_id` and `user_id` are too granular and `post_time` is high cardinality.

**Reference:** https://docs.databricks.com/en/delta/delta-partitioning.html

</details>

---

## Question 119

The downstream consumers of a Delta Lake table have complained that invalid latitude and longitude values in `activity_details` are breaking geolocation processes. A junior engineer attempts to add the following CHECK constraint:

```sql
ALTER TABLE activity_details
ADD CONSTRAINT valid_coordinates
CHECK (
  latitude  >= -90  AND latitude  <=  90 AND
  longitude >= -180 AND longitude <= 180
);
```

A senior engineer confirms the logic is correct, but the statement fails when executed. Why?

- **A.** The current table schema does not contain the field `valid_coordinates`; schema evolution must be enabled first.
- **B.** The table already exists; CHECK constraints can only be added during table creation.
- **C.** Some existing rows violate the constraint, and Delta requires all current records to satisfy a CHECK before adding it.
- **D.** CHECK constraints can be added only before inserting any data.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta validates existing data when adding a CHECK constraint; if any row violates the latitude or longitude ranges, the statement fails until those records are corrected.

**Reference:** https://docs.databricks.com/en/delta/delta-constraints.html

</details>

---

## Question 120

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.

Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

- **A.** Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
- **B.** Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum observed execution time ensures no records are dropped.
- **C.** The trigger interval cannot be modified without modifying the checkpoint directory; instead increase shuffle partitions to maximize parallelism.
- **D.** Use the trigger-once option and schedule the job every 10 seconds so backlogged records are processed with each batch.
- **E.** Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Smaller triggers shrink batch sizes, reducing processing time variance and backlog risk.

**Reference:** https://docs.databricks.com/en/structured-streaming/triggers.html

</details>

---

## Question 121

Which statement is true for Delta Lake?

- **A.** Views maintain caches of the latest table versions.
- **B.** Primary/foreign keys prevent duplicates.
- **C.** Delta automatically collects statistics for the first 32 columns to enable data skipping on filtered columns.
- **D.** Z-order applies only to numeric columns.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta computes column-level min/max stats for the first 32 columns in each file to accelerate predicate pruning.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html

</details>

---

## Question 122

Given this MERGE logic implementing Slowly Changing Dimension behavior:

```sql
MERGE INTO customers
USING (
  SELECT updates.customer_id AS merge_key, updates.*
  FROM updates
  UNION ALL
  SELECT NULL AS merge_key, updates.*
  FROM updates
  JOIN customers
    ON updates.customer_id = customers.customer_id
   WHERE customers.current = true
     AND updates.address <> customers.address
) staged_updates
ON customers.customer_id = merge_key
WHEN MATCHED AND customers.current = true
     AND customers.address <> staged_updates.address THEN
  UPDATE SET current = false,
             end_date = staged_updates.effective_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, current, effective_date, end_date)
  VALUES (staged_updates.customer_id,
          staged_updates.address,
          true,
          staged_updates.effective_date,
          null);
```

What type of table is `customers`?

- **A.** Type 2 table overwriting old values
- **B.** Type 2 table keeping history by closing old rows and inserting new ones
- **C.** Type 0 append-only table
- **D.** Type 1 table overwriting records in place

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The merge marks previous versions as `current = false` and inserts a new row, preserving history—characteristic of Type 2 SCDs.

**Reference:** https://docs.databricks.com/en/delta/delta-slow-change-tables.html

</details>

---

## Question 123

DLT pipeline tables share many expectations. How can the team reuse rules across tables?

- **A.** Apply constraints via an external job.
- **B.** Use global variables.
- **C.** Maintain expectations in a separate notebook or Python module that each DLT notebook imports.
- **D.** Store rules in a Delta table.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Packaging reusable expectation logic in a shared module allows multiple table definitions to import and apply the same checks.

**Reference:** https://docs.databricks.com/en/delta-live-tables/expectations.html

</details>

---

## Question 124

A new engineer needs to review production notebooks without risking changes. What is the highest permission you can grant?

- **A.** Can Manage
- **B.** Can Edit
- **C.** Can Run
- **D.** Can Read

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `Can Read` lets the engineer open the notebook to inspect code without the ability to run or modify it.

**Reference:** https://docs.databricks.com/en/notebooks/notebook-permissions.html

</details>

---

## Question 125

Given the view:

```sql
CREATE VIEW email_ltv AS
SELECT CASE WHEN is_member('marketing') THEN email ELSE 'REDACTED' END AS email,
       ltv
FROM user_ltv;
```

How does the view behave for an analyst not in the marketing group?

- **A.** Three columns are returned with one named `REDACTED`.
- **B.** Email and LTV are returned but email is null.
- **C.** Email and LTV are returned unchanged.
- **D.** Only email and LTV are returned; email contains the literal "REDACTED" for every row.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The CASE expression masks emails by substituting `'REDACTED'` whenever the user is not in the marketing group.

**Reference:** https://docs.databricks.com/en/sql/language-manual/functions/is_member.html

</details>

---

## Question 126

The data governance team requires every PII table to include column comments, a table comment, and the custom table property `contains_pii = true`. The following table is created:

```sql
CREATE TABLE dev.pii_test (
  id   INT,
  name STRING COMMENT "PII"
)
COMMENT "Contains PII"
TBLPROPERTIES('contains_pii' = true);
```

Which command allows you to manually confirm that all three requirements are satisfied?

- **A.** `DESCRIBE EXTENDED dev.pii_test`
- **B.** `DESCRIBE DETAIL dev.pii_test`
- **C.** `SHOW TBLPROPERTIES dev.pii_test`
- **D.** `DESCRIBE HISTORY dev.pii_test`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `DESCRIBE EXTENDED` reports column comments, table comments, and table properties in a single statement.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html

</details>

---

## Question 127

Deleting users with:

```sql
DELETE FROM users
WHERE user_id IN (SELECT user_id FROM delete_requests);
```

Does this guarantee the records are gone?

- **A.** Yes, deletes are permanent immediately.
- **B.** No; deleted data can still be accessed via time travel until VACUUM removes old files.
- **C.** Yes, because the Delta cache refreshes instantly.
- **D.** No; DELETE only works when combined with MERGE.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Rows are hidden from the latest version but remain accessible via previous versions until the retention period expires and VACUUM purges the files.

**Reference:** https://docs.databricks.com/en/delta/delta-retention.html

</details>

---

## Question 128

The data architect decided that once data lands in the Lakehouse, table access controls will govern all production tables and views. To let the core engineering group query a production database, the following grants were issued:

```sql
GRANT USAGE ON DATABASE prod TO eng;
GRANT SELECT ON DATABASE prod TO eng;
```

Assuming these are the only privileges that `eng` has and the users are not workspace admins, which statement describes their access?

- **A.** Group members can create, query, and modify all tables and views in `prod`, but cannot define custom functions.
- **B.** Group members can list all tables in `prod` but cannot query them.
- **C.** Group members can query and modify tables and views in `prod`, but cannot create new objects.
- **D.** Group members can query all tables and views in `prod`, but cannot create or edit anything in the database.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `USAGE` + `SELECT` allows read-only access to tables and views in the database.

**Reference:** https://docs.databricks.com/en/security/access-control/tables.html

</details>

---

## Question 129

A user wants to validate that a derived table `report` contains all rows from `validation_copy` using DLT expectations. The engineer initially tried:

```sql
CREATE LIVE TABLE report (
  CONSTRAINT no_missing_records EXPECT (key IN validation_copy)
)
AS SELECT <...>;
```

Which approach will successfully check that all expected records exist?

- **A.** Define a temporary table that left-joins `validation_copy` to `report` and assert no null report keys.
- **B.** Use a SQL UDF to compare the tables in an expectation.
- **C.** Create a view that left-joins the tables and reference it in expectations.
- **D.** Define a function to join the tables and check it in an expectation.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** DLT expectations must operate on the table being defined, so creating an intermediate table that performs the join and asserts completeness satisfies the requirement.

**Reference:** https://docs.databricks.com/en/delta-live-tables/expectations.html

</details>

---

## Question 130

A developer measures performance by repeatedly running interactive cells with `display()`. How can they get more realistic timings?

- **A.** Use Jobs UI because Photon only runs on job clusters.
- **B.** Only production-sized data and clusters work.
- **C.** Develop in an IDE against local Spark.
- **D.** Recognize that `display()` forces actions and caching makes repeated runs unrepresentative; instead, use realistic workloads (e.g., `Run All` or `%run`) to measure end-to-end behavior.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Displaying intermediate results triggers additional jobs and caching, so measuring entire runs (e.g., via `Run All` or scheduled jobs) provides accurate production timing.

**Reference:** https://docs.databricks.com/en/notebooks/notebooks-use.html#run-all-cells

</details>

---

## Question 131

Which statement describes Delta Lake Auto Compaction?

- **A.** An asynchronous job runs after the write completes to detect if files should be compacted; if so, `OPTIMIZE` rewrites them toward 1 GB.
- **B.** Before a jobs cluster terminates, `OPTIMIZE` is executed on all modified tables.
- **C.** Optimized writes use logical partitions instead of directory partitions.
- **D.** Data is queued in a messaging bus and committed in one batch when the job completes.
- **E.** An asynchronous job runs after the write completes to detect if files should be compacted; if so, it coalesces them toward ~128 MB files.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Auto Compaction runs asynchronously to combine small files into ~128 MB Parquet files, improving query performance without manual `OPTIMIZE` jobs.

**Reference:** https://docs.databricks.com/en/delta/delta-optimizations.html#auto-optimize

</details>

---

## Question 132

Where in the Spark UI can one diagnose a performance issue caused by missing predicate pushdown?

- **A.** Executor logs by grepping for “predicate push-down”
- **B.** Stage Detail screen by inspecting Input size
- **C.** Query Detail screen by examining the Physical Plan
- **D.** Delta transaction log column statistics

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** The Spark SQL tab (Query Detail) displays the physical plan; if filters appear after scans, predicate pushdown isn’t happening.

**Reference:** https://docs.databricks.com/en/jobs/monitor-run.html#use-the-spark-ui

</details>

---

## Question 133

A data engineer needs to capture the JSON spec of an existing Delta Live Tables pipeline so they can version it and create another pipeline from it. Which CLI command should they use?

- **A.** `databricks pipelines list`
- **B.** Stop the pipeline and use `reset`
- **C.** `databricks pipelines get --pipeline-id <id>` to fetch the spec, remove the `pipeline_id`, rename, and use it in a `create` call
- **D.** `databricks pipelines clone`

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `pipelines get` returns the JSON definition; editing it before invoking `pipelines create` lets you version and recreate pipelines.

**Reference:** https://docs.databricks.com/en/delta-live-tables/delta-live-tables-cli.html

</details>

---

## Question 134

Which Python variable lists directories that Python searches when importing modules?

- **A.** `importlib.resource_path`
- **B.** `sys.path`
- **C.** `os.path`
- **D.** `pypi.path`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** `sys.path` is the array of directories that the Python interpreter scans when resolving module imports.

**Reference:** https://docs.python.org/3/library/sys.html#sys.path

</details>

---

## Question 135

You test a function that integrates another function: `assert(myIntegrate(lambda x: x*x, 0, 3)[0] == 9)`. What kind of test is this?

- **A.** Unit
- **B.** Manual
- **C.** Functional
- **D.** Integration

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** The test exercises a single function in isolation with deterministic inputs, so it’s a unit test.

**Reference:** https://martinfowler.com/articles/practical-test-pyramid.html#UnitTests

</details>

---

## Question 136

What is a key benefit of an end-to-end test?

- **A.** Easier automation of the test suite
- **B.** Pinpoints errors in individual building blocks
- **C.** Provides coverage for all branches
- **D.** Closely simulates real-world usage of the application

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** End-to-end tests validate the entire workflow from inputs to outputs, best reflecting real user behavior.

**Reference:** https://martinfowler.com/articles/practical-test-pyramid.html#EndToEndTests

</details>

---

## Question 137

Which REST API call lists the tasks (e.g., notebooks) configured in a multi-task job?

- **A.** `/jobs/runs/list`
- **B.** `/jobs/list`
- **C.** `/jobs/runs/get`
- **D.** `/jobs/get`

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `jobs/get` retrieves the job definition including its tasks; `/jobs/list` gives IDs only and `/jobs/runs/*` describe executions.

**Reference:** https://docs.databricks.com/en/workflows/jobs/jobs-api.html#get-a-job

</details>

---

## Question 138

A data engineer wants to run unit tests on Python functions defined across Databricks notebooks. How can they do this while using representative data?

- **A.** Define and import test functions from another notebook
- **B.** Define and unit test functions using files stored in Repos, then run tests against non-production data that mirrors production
- **C.** Run tests directly against production data
- **D.** Define tests and functions in the same notebook

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Storing reusable code and tests in Repos files lets you import functions into notebooks and run unit tests against safe, representative datasets.

**Reference:** https://docs.databricks.com/en/repos/index.html

</details>

---

## Question 139

A data engineer wants to refactor repetitive DLT table definitions:

```python
@dlt.table(name=f"t1_dataset")
def t1_dataset():
    return spark.read.table("t1")

@dlt.table(name=f"t2_dataset")
def t2_dataset():
    return spark.read.table("t2")

@dlt.table(name=f"t3_dataset")
def t3_dataset():
    return spark.read.table("t3")
```

They attempt a parameterized approach:

```python
tables = ["t1", "t2", "t3"]
for t in tables:
    @dlt.table(name=f"{t}_dataset")
    def new_table():
        return spark.read.table(t)
```

When the pipeline runs, the DAG shows incorrect configuration values for these tables. How can the engineer fix this?

- **A.** Wrap the loop inside another table definition.
- **B.** Convert the table list into a dictionary of settings.
- **C.** Move the table creation logic into a helper function and call it with different parameters instead of defining decorators inside a loop.
- **D.** Load configuration values from an external file.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** DLT decorators must be bound at definition time; defining them inside a loop reuses the last value. A helper function that returns a table definition for each parameter avoids this problem.

**Reference:** https://docs.databricks.com/en/delta-live-tables/python.html

</details>

---

## Question 140

Which statement characterizes the Structured Streaming programming model?

- **A.** Uses GPUs for parallel throughput
- **B.** Derived from Apache Kafka’s messaging bus
- **C.** Uses specialized hardware for sub-second latency
- **D.** Models new data as rows appended to an unbounded table
- **E.** Relies on distributed nodes holding incremental state for cached stages

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Structured Streaming treats incoming data as incremental append-only rows to an unbounded table, allowing SQL-like operations over streams.

**Reference:** https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview

</details>

---

## Question 141

Which configuration parameter directly affects the size of a Spark partition on ingest?

- **A.** `spark.sql.files.maxPartitionBytes`
- **B.** `spark.sql.autoBroadcastJoinThreshold`
- **C.** `spark.sql.files.openCostInBytes`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `maxPartitionBytes` controls how many bytes Spark packs into each partition when reading files.

**Reference:** https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options

</details>

---

## Question 142

In the Spark UI, task durations within a stage show max duration 100× larger than median. What likely causes the slowdown?

- **A.** Task queuing
- **B.** Spill due to insufficient disk
- **C.** Network latency
- **D.** Data skew assigning more data to some partitions
- **E.** Credential validation errors

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Large variance in task durations typically indicates skew, where certain partitions contain far more data than others.

**Reference:** https://docs.databricks.com/en/optimizations/skew.html

</details>

---

## Question 143

For a job with a wide transformation, which cluster configuration delivers maximum performance (400 GB RAM, 160 cores total)?

- **A.** 1 VM, 400 GB RAM, 160 cores
- **B.** 8 VMs, 50 GB RAM, 20 cores each
- **C.** 16 VMs, 25 GB RAM, 10 cores each
- **D.** 4 VMs, 100 GB RAM, 40 cores each
- **E.** 2 VMs, 200 GB RAM, 80 cores each

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** More, smaller executors (16×25 GB/10 cores) increase parallelism and reduce shuffle contention for wide operations.

**Reference:** https://docs.databricks.com/en/clusters/configure.html#choose-the-right-number-of-workers

</details>

---

## Question 144

A Delta MERGE is used to ingest a batch of records from `new_events` into the `events` table. The view `new_events` has the same schema as `events`, and `event_id` is the unique key. The logic is:

```sql
MERGE INTO events
USING new_events
ON events.event_id = new_events.event_id
WHEN NOT MATCHED THEN INSERT *;
```

When this query is executed, what happens to new records that have the same `event_id` as an existing record?

- **A.** They are merged
- **B.** They are ignored
- **C.** They are updated
- **D.** They are inserted
- **E.** They are deleted

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** With no `WHEN MATCHED` clause, matching keys are skipped, so duplicates are ignored.

**Reference:** https://docs.databricks.com/en/delta/merge.html

</details>

---

## Question 145

A junior data engineer wants to use Delta Change Data Feed (CDF) to create a Type 1 history table that records every value ever written to a bronze table (created with `delta.enableChangeDataFeed = true`). The engineer schedules the following daily job:

```python
from pyspark.sql.functions import col

(spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 0)
      .table("bronze")
      .filter(col("_change_type").isin(["update_postimage", "insert"]))
      .write
      .mode("append")
      .table("bronze_history_type1"))
```

What happens when this job runs multiple times?

- **A.** Merges updated records, overwriting previous values
- **B.** Replays the entire change history each run, appending duplicates
- **C.** Overwrites the target table with the entire history
- **D.** Calculates differences between original and current versions
- **E.** Appends only changes since the previous run

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Since `startingVersion` is always 0, every run reprocesses the full change feed, duplicating previously ingested changes.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html

</details>

---

## Question 146

A critical Kafka field was omitted when writing data to Delta Lake and downstream storage; Kafka retains only seven days, while the pipeline has been running for three months. How can Delta Lake prevent this type of data loss going forward?

- **A.** The Delta log and Structured Streaming checkpoints record the full history of the Kafka producer.
- **B.** Delta Lake schema evolution can retroactively calculate the missing values if they were present upstream.
- **C.** Delta Lake automatically validates that all source fields are ingested.
- **D.** Landing all raw Kafka data and metadata into a bronze Delta table creates a permanent, replayable history even if later stages omit a column.
- **E.** Data can never be deleted from Delta Lake, so loss is impossible.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Persisting every source column in a bronze Delta table provides an immutable history that can be replayed if downstream logic misses fields.

**Reference:** https://docs.databricks.com/en/delta/bronze-silver-gold.html

</details>

---
## Question 147

When scheduling Structured Streaming jobs for production, which configuration automatically recovers from query failures while keeping costs low?

- **A.** Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: Unlimited
- **B.** Cluster: New Job Cluster; Retries: None; Maximum Concurrent Runs: 1
- **C.** Cluster: Existing All-Purpose Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1
- **D.** Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1
- **E.** Cluster: Existing All-Purpose Cluster; Retries: None; Maximum Concurrent Runs: 1

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** A job cluster is created only for the run, so you are billed only while processing; pairing it with unlimited retries and a single concurrent run lets Databricks automatically relaunch the query after failures without leaving idle compute running.

**Reference:** https://docs.databricks.com/en/structured-streaming/deploy.html#schedule-structured-streaming-jobs

</details>

## Question 148

A nightly job ingests data into a Delta Lake table using the following helper function:

```python
from pyspark.sql.functions import current_timestamp, input_file_name, col
from pyspark.sql.column import Column

def ingest_daily_batch(time_col: Column, year: int, month: int, day: int):
    (spark.read
         .format("parquet")
         .load(f"/mnt/daily_batch/{year}/{month}/{day}")
         .withColumn("ingest_time", time_col)
         .withColumn("source_file", input_file_name())
         .write
         .mode("append")
         .table("bronze"))
```

The next step in the pipeline requires a function that returns only the new records that have not yet been processed further. Which snippet completes `def new_records():`?

- **A.** `return spark.readStream.table("bronze")`
- **B.** `return spark.read.table("bronze")`
- **C.** `return spark.readStream.load("bronze")`
- **D.** `return spark.read.option("readChangeFeed", "true").table("bronze")`

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Reading the table with Change Data Feed enabled yields only the new inserts and update post-images that have not been processed yet, which is what the downstream function needs.

**Reference:** https://docs.databricks.com/en/delta/delta-change-data-feed.html

</details>

## Question 149

A junior data engineer is implementing the `silver_device_recordings` table from a highly nested JSON source that contains 100 fields, 45 of which are already used by downstream dashboards and models. Which statement accurately informs the schema-design decision?

- **A.** Tungsten optimizations for string data mean it is always best to store JSON in raw string form.
- **B.** Because Delta stores data in Parquet, you can evolve types simply by editing the Parquet footers.
- **C.** Human effort is the largest cost in data engineering, so schema declaration should always be automated.
- **D.** Databricks infers permissive types to accommodate observed data; manually declaring the schema provides stronger data-quality enforcement.
- **E.** Schema inference and evolution always match downstream system types.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Automatic inference picks types that fit any observed values and may allow unexpected structures; explicitly defining the schema enforces data quality and ensures downstream contracts are met.

**Reference:** https://docs.databricks.com/en/delta/delta-schema.html#specify-the-schema

</details>

## Question 150

The data engineering team maintains the following code:

```python
accountDF = spark.table("accounts")
orderDF   = spark.table("orders")
itemDF    = spark.table("items")

orderWithItemDF = (orderDF
    .join(itemDF, orderDF.itemID == itemDF.itemID)
    .select("accountID", "itemID", "orderID", "itemName"))

finalDF = (accountDF
    .join(orderWithItemDF, accountDF.accountID == orderWithItemDF.accountID)
    .select(orderWithItemDF["*"], accountDF.city))

(finalDF.write
        .mode("overwrite")
        .table("enriched_itemized_orders_by_account"))
```

Assuming the joins and input data are correct, what happens when this job runs?

- **A.** A batch job updates only changed rows in `enriched_itemized_orders_by_account`.
- **B.** The table `enriched_itemized_orders_by_account` is fully overwritten with the latest join results.
- **C.** The job becomes incremental and uses state to add only new rows.
- **D.** The job waits for queries before materializing results.
- **E.** An incremental job recalculates only if new rows arrive in the sources.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Writing with `mode("overwrite")` replaces the entire target table with the newly computed join output every time the job runs.

**Reference:** https://docs.databricks.com/en/delta/delta-batch.html#overwrite-table

</details>

## Question 151

The organization is migrating thousands of objects into bronze, silver, and gold tables. Bronze supports engineering workloads, silver feeds engineering and ML, and gold serves BI. PII exists everywhere but is anonymized at silver and gold. Which practice best balances security and collaboration?

- **A.** Isolate tables into separate databases for each data-quality tier to simplify ACLs and physical storage boundaries.
- **B.** Database organization has no effect on security or discoverability.
- **C.** Store all production tables in a single database and grant everyone view privileges.
- **D.** Use only the default database for managed table security.
- **E.** Plan to create thousands of databases because tables must live in their database’s storage container.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Organizing medallion tiers into separate schemas allows clear ACL boundaries and aligns with best practices for shared Lakehouse environments.

**Reference:** https://learn.microsoft.com/azure/databricks/lakehouse/medallion-architecture

</details>

## Question 152

The data architect mandates that every Lakehouse table be external (unmanaged). How can the team ensure this?

- **A.** Specify a `LOCATION` clause whenever creating a database.
- **B.** Configure an external data warehouse and use Databricks only for ELT.
- **C.** Specify a `LOCATION` every time `CREATE TABLE` is executed so the path points to external storage.
- **D.** Use the `EXTERNAL` keyword in `CREATE TABLE`.
- **E.** Mount external storage during workspace setup.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Pointing each `CREATE TABLE` statement to an explicit storage path registers the table as unmanaged, regardless of database defaults.

**Reference:** https://docs.databricks.com/en/tables/managed-and-unmanaged-tables.html

</details>

## Question 153

An aggregate table used across teams must add and rename columns to satisfy a customer-facing application. How can engineers minimize disruption without increasing the number of managed tables?

- **A.** Notify all users that the schema will change and ask them to adjust.
- **B.** Write the revised schema to a new table for the application and create a view on top that preserves the original schema and table name for other teams.
- **C.** Deep clone the table and keep clones synchronized.
- **D.** Replace the current table with a view and create a brand new application table.
- **E.** Add a comment warning of upcoming schema changes and overwrite the table in place.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Maintaining compatibility via a view lets dependent teams continue using the old schema while the new table satisfies the application requirements.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html

</details>

## Question 154

A Delta Lake table has schema `user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE` and is partitioned by `date`. A query filters on `longitude < 20 AND longitude > -20`. How does Delta filter the data?

- **A.** Delta uses transaction-log statistics to skip entire partitions.
- **B.** No skipping occurs because longitude is not the partition column.
- **C.** The engine uses row-level stats stored in the log to identify matching rows.
- **D.** Transaction-log statistics are used to skip Parquet files whose longitude ranges fall outside the predicate.
- **E.** Parquet footers are scanned row by row.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Delta records min/max stats for each file in the transaction log, enabling file-level skipping even when filtering on non-partition columns.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html

</details>

## Question 155

A U.S. company stores all data in a U.S. cloud region but hires contractors in India. Where should the Databricks workspace for the contractors be deployed?

- **A.** Always in the same region because Databricks runs HDFS on cloud volumes.
- **B.** Any region works because workspaces are region-independent.
- **C.** In the same region as the data to avoid cross-region latency and egress costs.
- **D.** Near the user workstations because they act as drivers.
- **E.** Near the end users to keep code secure in transit.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Cross-region access adds latency and egress fees; Databricks clusters should be created in the same region as the underlying storage whenever possible.

**Reference:** https://learn.microsoft.com/azure/databricks/administration-guide/account-settings/azure-databricks-workspace-configuration#choose-workspace-region

</details>

## Question 156

Teams complain that invalid latitude/longitude values in `activity_details` break downstream processing. Engineers attempt to add this constraint:

```sql
ALTER TABLE activity_details
ADD CONSTRAINT valid_coordinates
CHECK (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180);
```

Why does the statement fail?

- **A.** Two-phase locking prevents the write because another team is querying the table.
- **B.** CHECK constraints can only be added when the table is created.
- **C.** Existing rows violate the constraint, and Delta validates the current data before adding it.
- **D.** CHECK constraints can only be added before inserting any data.
- **E.** The schema lacks a `valid_coordinates` column.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta enforces constraints immediately and refuses to add one if any existing row violates it; offending records must be cleaned first.

**Reference:** https://docs.databricks.com/en/delta/delta-constraints.html

</details>

## Question 157

Which statement about Delta Lake and the Lakehouse is true?

- **A.** Parquet compresses row by row, so repeated strings are never compressed.
- **B.** Delta automatically collects statistics on the first 32 columns, enabling data skipping for filtered queries.
- **C.** Views maintain cached snapshots of source tables.
- **D.** Primary and foreign keys prevent duplicates in Delta tables.
- **E.** Z-ordering applies only to numeric columns.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Delta stores min/max stats for the first 32 columns of each file to allow the optimizer to skip data files when queries contain selective predicates.

**Reference:** https://docs.databricks.com/en/delta/delta-data-skipping.html

</details>

## Question 158

A Databricks SQL alert executes the following query every minute:

```sql
SELECT mean(temperature), max(temperature), min(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id;
```

The alert fires when `mean(temperature) > 120` and notifications are throttled to once per minute. If notifications occur three minutes in a row and then stop, what must be true?

- **A.** The overall average for all sensors exceeded 120 during three runs.
- **B.** The table was unresponsive for three consecutive runs.
- **C.** The query failed to refresh for three minutes.
- **D.** The maximum temperature for a sensor exceeded 120.
- **E.** At least one sensor’s average temperature exceeded 120 in three consecutive refreshes.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Alerts evaluate each grouped row independently; repeated notifications mean the condition held for some `sensor_id` on three successive executions.

**Reference:** https://docs.databricks.com/en/sql/user/alerts.html#threshold-alerts

</details>

## Question 159

The view `updates` holds incremental records to apply to the `customers` table. The following logic processes the batch:

```sql
MERGE INTO customers
USING (
  SELECT updates.customer_id AS merge_key, updates.*
  FROM updates
  UNION ALL
  SELECT NULL AS merge_key, updates.*
  FROM updates
  JOIN customers ON updates.customer_id = customers.customer_id
  WHERE customers.current = true AND updates.address <> customers.address
) staged_updates
ON customers.customer_id = merge_key
WHEN MATCHED AND customers.current = true
     AND customers.address <> staged_updates.address THEN
  UPDATE SET current = false, end_date = staged_updates.effective_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, address, current, effective_date, end_date)
  VALUES (staged_updates.customer_id, staged_updates.address, true, staged_updates.effective_date, null);
```

Which statement describes this implementation?

- **A.** Type 3 table: old values are stored as extra columns alongside current values.
- **B.** Type 2 table: previous rows are marked not current and new rows are inserted.
- **C.** Type 0 table: writes are append-only with no modifications.
- **D.** Type 1 table: rows are overwritten in place and history is not kept.
- **E.** Type 8 table: old values are overwritten and new customers appended.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Matching current rows are closed out (`current = false`, `end_date` set) and a new current row is inserted, which is the textbook Type 2 slowly changing dimension pattern.

**Reference:** https://docs.databricks.com/en/delta/delta-slow-change-tables.html

</details>

## Question 160

Which description best characterizes a Type 2 slowly changing dimension table?

- **A.** Old values are stored as additional columns on the same row.
- **B.** Previous rows are retained, marked as not current, and new current rows are inserted.
- **C.** All writes are append-only with no updates.
- **D.** Rows are overwritten in place with no history.
- **E.** Old values are overwritten and new customers appended.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Type 2 SCDs preserve full history by versioning rows—closing the previous record and inserting a new current one whenever an attribute changes.

**Reference:** https://docs.databricks.com/en/delta/delta-slow-change-tables.html

</details>

## Question 161

A table named `user_ltv` feeds a view that exposes customer LTV to multiple analyst teams. Access is controlled with ACL groups. The table schema is `email STRING, age INT, ltv INT` and the view definition is:

```sql
CREATE VIEW email_ltv AS
SELECT CASE WHEN is_member('marketing') THEN email ELSE 'REDACTED' END AS email,
       age,
       ltv
FROM user_ltv;
```

An analyst who is not a member of the `marketing` group runs `SELECT * FROM email_ltv;`. What result will they see?

- **A.** Three columns are returned but one is named `REDACTED` and contains only nulls.
- **B.** Only `email` and `ltv` are returned and `email` is null.
- **C.** `email`, `age`, and `ltv` are returned with their original values.
- **D.** Only `age` and `ltv` are returned.
- **E.** `email`, `age`, and `ltv` are returned, but the `email` column contains the literal `REDACTED` for every row.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** The `CASE` expression masks the email when the user is not in the marketing group, so all rows are returned but the `email` column contains the string `REDACTED`.

**Reference:** https://docs.databricks.com/en/sql/language-manual/functions/is_member.html

</details>

---

## Question 162

The data governance team requires that all tables containing PII include column comments, a table comment, and a custom table property `contains_pii = true`. The table is created with:

```sql
CREATE TABLE dev.pii_test (
  id   INT,
  name STRING COMMENT 'PII'
)
COMMENT 'Contains PII'
TBLPROPERTIES('contains_pii' = true);
```

Which command confirms that these annotations were applied?

- **A.** `DESCRIBE EXTENDED dev.pii_test`
- **B.** `DESCRIBE DETAIL dev.pii_test`
- **C.** `SHOW TBLPROPERTIES dev.pii_test`
- **D.** `DESCRIBE HISTORY dev.pii_test`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `DESCRIBE EXTENDED` returns the column comments, table comment, and table properties in a single statement, allowing the reviewer to verify all annotations.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-describe-table.html

</details>

---

## Question 163

A DELETE statement removes users in `delete_requests` from the `users` Delta table:

```sql
DELETE FROM users
WHERE user_id IN (SELECT user_id FROM delete_requests);
```

Does this guarantee the records are irretrievable, and why?

- **A.** Yes; Delta ACID semantics permanently remove the data immediately.
- **B.** No; old versions remain accessible via time travel until they are vacuumed.
- **C.** Yes; once cached results expire the rows are gone forever.
- **D.** No; DELETE works only when paired with MERGE.
- **E.** No; the Delta cache may continue to return stale rows even after rebooting the cluster.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** DELETE hides rows from the latest version, but the underlying data files are retained until the retention period elapses and `VACUUM` removes them, so historic versions can still be queried.

**Reference:** https://docs.databricks.com/en/delta/delta-retention.html

</details>

---

## Question 164

The finance team mounts `/mnt/finance_eda_bucket` and creates the database `finance_eda_db` with that location. After granting `USAGE` and `CREATE` to the `finance` group, a user runs:

```sql
CREATE TABLE finance_eda_db.tx_sales AS
SELECT *
FROM sales
WHERE state = 'TX';
```

How is `tx_sales` stored?

- **A.** As a logical view in the metastore’s control plane.
- **B.** As an external table referencing `/mnt/finance_eda_bucket`.
- **C.** As a logical table storing both plans in the metastore.
- **D.** As a managed table whose data files live under `/mnt/finance_eda_bucket`.
- **E.** As a managed table in the DBFS root.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Specifying a `LOCATION` in the database definition means new managed tables inherit that path, so the CTAS statement writes Delta files into `/mnt/finance_eda_bucket`.

**Reference:** https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-database.html

</details>

---

## Question 165

Databricks Secrets store credentials securely, but which limitation still applies?

- **A.** Secrets are hashed with SHA256 and can be reversed.
- **B.** Account admins can view all secrets in plain text from the Accounts console.
- **C.** Secrets live in the Hive metastore and DBAs can query them.
- **D.** Reading a secret character by character (e.g., iterating over `dbutils.secrets.get`) prints the value in plain text in the notebook output.
- **E.** The REST API returns secret values for any PAT with `secrets:list` scope.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Secrets are protected only when referenced as a whole; printing or iterating over the returned string will expose it in the notebook output, so users must handle returned values carefully.

**Reference:** https://docs.databricks.com/en/security/secrets/secrets.html#best-practices

</details>

---

## Question 166

What is true about Databricks job run history retention?

- **A.** Run history is retained indefinitely until manually deleted.
- **B.** Run history is retained for 30 days and then deleted permanently.
- **C.** Run history (including notebook output) is retained for 60 days, during which you can export logs (e.g., HTML/PDF).
- **D.** Run history is retained for 7 days unless copied to DBFS.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Databricks stores job run details for 60 days; during that window you can download notebook results or logs before they expire.

**Reference:** https://learn.microsoft.com/azure/databricks/workflows/jobs/jobs#job-run-details

</details>

---

## Question 167

User A creates Databricks jobs via the REST API. User B triggers those jobs via an external orchestrator that also calls the REST API. Both authenticate with their own personal access tokens. What appears in the workspace audit logs?

- **A.** A service principal identity because the REST API was used.
- **B.** Only User B’s identity for both the creation and the runs.
- **C.** Separate entries: job creation events attributed to User A and job run events attributed to User B.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Audit logs record the identity attached to each PAT; API calls made with User A’s token appear under their name, while runs triggered with User B’s token are attributed to User B.

**Reference:** https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html

</details>

---

## Question 168

A new Databricks user measures performance by running cells repeatedly with `display()` after each transformation. Which adjustment yields a more accurate view of production performance?

- **A.** Rewrite all logic in Scala and compile to JARs.
- **B.** Only test with production-sized data and clusters using Run All.
- **C.** Move development to a local Spark build for benchmarking.
- **D.** Avoid relying on `display()` results; it triggers actions and caching, so measuring end-to-end runs (e.g., `Run All` or scheduled jobs) gives realistic timings.
- **E.** Use the Jobs UI because Photon only activates on job clusters.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `display()` executes an action and caches intermediate results, so repeatedly running cells underestimates cost. Executing the entire workload as a job or via `Run All` better reflects production behavior.

**Reference:** https://docs.databricks.com/en/notebooks/notebooks-use.html#run-all-cells

</details>

---

## Question 169

A developer working in Databricks Repos is on a stale personal branch and cannot see branch `dev-2.3.9` in the notebook dropdown. How can they review the latest logic?

- **A.** Use Repos’ branch picker to create a pull request automatically.
- **B.** Run `git fetch` and `git checkout dev-2.3.9` from the Repos terminal (or the “Switch branch” dialog) to pull the remote branch locally, then open the notebook on that branch.
- **C.** Delete the repo and reclone it.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Repos mirrors Git behavior; fetching updates and checking out the desired branch (via the UI or terminal) makes the branch available so the developer can open the notebook version they need.

**Reference:** https://docs.databricks.com/en/repos/git-operations.html

</details>

---

## Question 170

A production cluster uses the same VM type for driver and executors (three executor nodes). Which Ganglia signal suggests the driver is the bottleneck?

- **A.** Five-minute load average remains flat.
- **B.** Network bytes received never exceeds 80 MB/s.
- **C.** Total disk usage stays constant.
- **D.** Network I/O never spikes.
- **E.** Overall cluster CPU utilization averages ~25% despite a backlog of work.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Low overall CPU utilization while the workload lags indicates the driver is saturated (e.g., single-threaded coordination) and cannot feed work to executors, even though they have spare capacity.

**Reference:** https://docs.databricks.com/en/clusters/clusters-monitor.html#view-cluster-metrics-in-ganglia

</details>

---
## Question 171

Where in the Spark UI can you confirm whether predicate push-down is (or is not) being applied to a query?

- **A.** In the executor log files by grepping for the text "predicate push-down"
- **B.** In the Stage Detail > Completed Stages table by inferring from the Input column size
- **C.** In the Storage Detail screen by noting which cached RDDs are stored on disk
- **D.** In the Delta Lake transaction log by looking at column statistics
- **E.** In the SQL tab's query detail screen by interpreting the physical plan

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** The Spark UI SQL tab shows each query's logical and physical plan, including the `PushedFilters` section that reveals whether predicates were pushed down to a data source. That's the authoritative place to confirm missing predicate push-down rather than inferring it indirectly from executor logs or stage-level metrics.

**Reference:** Spark documentation – Web UI SQL Tab (https://spark.apache.org/docs/latest/web-ui.html#sql-tab)

</details>

---
## Question 172

You execute the following line in a Databricks notebook and receive an `AnalysisException` stating `cannot resolve 'heartrateheartrateheartrate' given input columns ...`.

```python
display(df.select(3 * "heartrate"))
```

Which statement describes the root cause of this error?

- **A.** The cell was run in a Scala notebook instead of a Python notebook.
- **B.** There is no column named `heartrateheartrateheartrate` in the table.
- **C.** Columns in PySpark cannot be multiplied together, so this is a type error.
- **D.** DataFrames cannot be multiplied, so this is a type error.
- **E.** The `heartrate` column is not referenced correctly as a column expression.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** In Python, multiplying a string literal by an integer repeats the string (`3 * "heartrate"` becomes the literal name `heartrateheartrateheartrate`). PySpark interprets bare strings in `select` as column names, so the query requests a non-existent column and Spark raises an `AnalysisException` saying it cannot resolve that column.

**Reference:** Python documentation – Sequence types (`str * n` repeats the string) (https://docs.python.org/3/library/stdtypes.html#common-sequence-operations)

</details>

---
## Question 173

Which distribution format does Databricks support for installing custom Python packages as workspace libraries?

- **A.** `sbt` packages
- **B.** CRAN or npm packages
- **C.** Python wheels (`.whl` files)
- **D.** JAR bundles only

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Databricks workspace libraries and `%pip` commands support installing Python wheels, including wheels uploaded to DBFS or workspace files. That's the supported packaging format for distributing custom Python code across clusters.

**Reference:** Databricks documentation – Workspace libraries (https://docs.databricks.com/en/libraries/workspace-libraries.html)

</details>

---
## Question 174

Which Python variable contains the ordered list of directories that are searched when importing modules?

- **A.** `importlib.resource_path`
- **B.** `sys.path`
- **C.** `os.path`
- **D.** `pypi.path`
- **E.** `pylib.source`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The `sys.path` list holds the interpreter's module search path and is consulted each time `import` resolves a module name. Modifying or inspecting `sys.path` lets you control where Python looks for packages.

**Reference:** Python documentation – `sys.path` (https://docs.python.org/3/library/sys.html#sys.path)

</details>

---
## Question 175

Implementing unit tests in a PySpark application requires upfront design effort. Which benefit offsets that investment?

- **A.** It improves the quality of the raw source data.
- **B.** It validates an entire end-to-end use case at once.
- **C.** It makes troubleshooting easier because each component is isolated and tested individually.
- **D.** It yields faster deployment and execution times by itself.
- **E.** It guarantees that all steps interact correctly in production.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Well-designed unit tests isolate individual pieces of logic so they can run independently of external systems. That isolation makes failures easier to diagnose because you can pinpoint the component that regressed instead of debugging the entire pipeline.

**Reference:** Microsoft Learn – Unit testing best practices (tests should be isolated and stand-alone) (https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices)

</details>

---
## Question 176

Which statement accurately describes integration testing?

- **A.** It validates that multiple subsystems work together correctly.
- **B.** It requires an automated testing framework.
- **C.** It always requires manual intervention.
- **D.** It validates an entire business use case end to end.
- **E.** It verifies the behavior of individual units in isolation.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Integration testing joins previously tested components or modules and verifies that their interactions behave as expected. It's distinct from isolated unit testing and from full end-to-end tests.

**Reference:** IBM documentation – Integration testing definition (https://www.ibm.com/topics/integration-testing)

</details>

---
## Question 177

Which Databricks REST API call returns the notebook tasks that are configured inside a multi-task job?

- **A.** `GET /api/2.1/jobs/runs/list`
- **B.** `GET /api/2.1/jobs/runs/get-output`
- **C.** `GET /api/2.1/jobs/runs/get`
- **D.** `GET /api/2.1/jobs/get`
- **E.** `GET /api/2.1/jobs/list`

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The `GET /api/2.1/jobs/get` endpoint returns the persisted job settings for a specific job ID, including the array of configured tasks and their notebook paths. The runs-related endpoints only report the status of executed runs, and `/jobs/list` provides metadata but not each task definition.

**Reference:** Databricks Jobs API reference (https://docs.databricks.com/api/workspace/jobs.html)

</details>

---
## Question 178

A Databricks job has three tasks. Task A has no dependencies. Tasks B and C both depend on task A and run in parallel. Task A and B finish successfully, but task C fails. What is the resulting state?

- **A.** Tasks A and B finish successfully, and any logic in task C may have partially executed before failing.
- **B.** Tasks A and B finish successfully, but Databricks automatically rolls back any work in task C.
- **C.** Only task A may have committed changes; dependent tasks never write when a later task fails.
- **D.** No task commits changes until all tasks succeed.
- **E.** Because task C failed, the entire job automatically rolls back all completed work.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Multi-task jobs execute each task independently according to the dependency graph. When one parallel task fails, its downstream dependents are skipped, but prior successful tasks keep their results—Databricks does not provide automatic cross-task rollback.

**Reference:** Databricks Jobs documentation – Multi-task job behavior (https://docs.databricks.com/en/workflows/jobs/jobs.html)

</details>

---
## Question 179

A Delta Lake table was created with `CREATE TABLE prod.sales_by_stor USING DELTA LOCATION '/mnt/prod/sales_by_store'`. To fix the misspelled name, the following command is run:

```sql
ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_store;
```

What happens after executing the command?

- **A.** The metastore entry is renamed; the Delta transaction log and data files remain untouched.
- **B.** The table rename is recorded as a new action in the Delta transaction log.
- **C.** All related files and metadata are dropped and recreated transactionally.
- **D.** The metastore entry is renamed and all data files are physically moved.
- **E.** A brand new Delta log is created for the renamed table.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `ALTER TABLE ... RENAME TO` only updates the catalog metadata so the table can be referenced under a new name. The table's storage location and transaction log stay in place, so the data itself is unaffected.

**Reference:** Databricks SQL language reference – `ALTER TABLE` rename (https://docs.databricks.com/en/sql/language-manual/delta-alter-table.html)

</details>

---
## Question 180

A security-conscious team stores a JDBC password in Databricks secrets and updates their notebook as follows (other options were already configured):

```python
password = dbutils.secrets.get(scope="db_creds", key="jdbc_password")
print(password)

df = (spark.read
        .format("jdbc")
        .option("url", connection)
        .option("dbtable", tableName)
        .option("user", username)
        .option("password", password))
```

What happens when this code runs?

- **A.** The connection fails and `[REDACTED]` is printed.
- **B.** An interactive prompt asks for the password again, then saves it to DBFS.
- **C.** An interactive prompt collects the password and prints it in plain text.
- **D.** The connection succeeds and the secret value is printed in plain text.
- **E.** The connection succeeds but printing the variable outputs `[REDACTED]` to avoid leaking the secret.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** `dbutils.secrets.get` returns the real secret so it can be passed to Spark configuration (so the JDBC connection succeeds), but Databricks automatically redacts the value whenever it is printed or echoed in logs, showing `[REDACTED]` instead of the actual password.

**Reference:** Databricks secrets documentation – secret values are redacted when printed (https://docs.databricks.com/en/security/secrets/secrets.html)

</details>

---
## Question 181

The nightly `store_sales_summary` Delta table stores rolling aggregates such as last-day sales, trailing-7-day totals, and year-to-date averages. All source facts live in `daily_store_sales`, which is implemented as a Type 1 table (auditors can edit historical `total_sales` values). What is the safest strategy to keep the aggregate table accurate?

- **A.** Recompute the aggregates from `daily_store_sales` each night and overwrite `store_sales_summary` in one batch job.
- **B.** Append only the newest day of aggregates to `store_sales_summary`.
- **C.** Perform nightly MERGE statements that only upsert rows touched that day.
- **D.** Switch to a Structured Streaming read and keep aggregates updated continuously.
- **E.** Subscribe to the change data feed from `daily_store_sales` so only changed records are applied.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** A Type 1 table overwrites historical records when corrections arrive, so the only reliable way to keep downstream gold tables consistent is to recompute the full aggregate from the curated source and replace the summary output in one ACID write. Databricks positions gold-layer tables as “consumption-ready” outputs that are rebuilt from trusted silver data for reporting accuracy, which is exactly what the nightly overwrite provides.

**Reference:** Databricks – *Medallion architecture* (gold tables are curated reporting outputs derived from refined sources) (https://www.databricks.com/glossary/medallion-architecture)

</details>

---
## Question 182

A teammate wants to schedule the following notebook as a production job:

1. `rawDF = spark.table("raw_data")`
2. `rawDF.printSchema()`
3. `flattenedDF = rawDF.select("*", "values.*")`
4. `finalDF = flattenedDF.drop("values")`
5. `finalDF.explain()`
6. `display(finalDF)`
7. `finalDF.write.mode("append").saveAsTable("flat_data")`

Which command should be removed before turning this into a job?

- **A.** Cmd 2
- **B.** Cmd 3
- **C.** Cmd 4
- **D.** Cmd 5
- **E.** Cmd 6

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** `display()` only renders interactive tables and charts inside the notebook UI; it has no effect when a job runs headless on a schedule. Removing the `display` call keeps the job focused on deterministic logic (read, transform, write) while avoiding an unnecessary UI operation.

**Reference:** Azure Databricks – *Create visualizations in notebooks* (the `display(df)` command is solely for interactive notebook output) (https://learn.microsoft.com/en-us/azure/databricks/notebooks/visualizations/index)

</details>

---
## Question 183

A dashboard must refresh every hour, and the ETL that produces its dataset finishes in roughly 10 minutes. Which configuration meets the SLA for the lowest cost?

- **A.** Run the notebook manually whenever analysts refresh the dashboard.
- **B.** Schedule an hourly Databricks job that spins up a new job cluster for the pipeline, then terminates it.
- **C.** Convert the pipeline to Structured Streaming with a 60-minute trigger.
- **D.** Keep an interactive cluster alive all day and trigger the job hourly on it.
- **E.** Fire the job via webhook whenever new data lands.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Databricks Jobs are designed to automate “frequent, repeatable tasks,” and job clusters provision just for the run and then shut down, so you only pay for the ~10 minutes of compute each hour instead of keeping an interactive cluster idle the rest of the time. That satisfies the hourly SLA with minimal cost.

**Reference:** Azure Databricks – *Jobs overview* (jobs let you schedule recurring workloads on ephemeral clusters) (https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/jobs)

</details>

---
## Question 184

A Databricks SQL dashboard reports the row count for several Delta tables using this pattern:

```sql
SELECT COUNT(*) FROM <table_name>;
```

How are the results produced each time the dashboard refreshes?

- **A.** Every data file in the table is scanned so Spark can count all rows.
- **B.** Cached values are returned unless `REFRESH` is issued on the table.
- **C.** The count is read directly from the Delta transaction log.
- **D.** The count is computed from Parquet footer metadata only.
- **E.** The count is read from the Hive metastore entry for the table.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Because `COUNT(*)` has no filter predicate, Delta Lake cannot use data-skipping metadata to prune files; Spark must scan every file in the table to aggregate the row count for each refresh cycle.

**Reference:** Azure Databricks – *Data skipping for Delta Lake* (statistics prune files only when a query includes a filter predicate) (https://learn.microsoft.com/en-us/azure/databricks/delta/data-skipping)

</details>

---
## Question 185

A managed Delta table is created as:

```sql
CREATE TABLE prod.sales_by_store AS
SELECT *
FROM prod.sales a
INNER JOIN prod.store b ON a.store_id = b.store_id;
```

What happens if a workspace admin runs `DROP TABLE prod.sales_by_store;`?

- **A.** Nothing until an explicit `COMMIT` is issued.
- **B.** The metastore entry is removed but the data files remain in storage.
- **C.** Both the catalog entry and the underlying data files are deleted.
- **D.** Delta Lake blocks the drop because it is a production table.
- **E.** The data is only soft-deleted and can still be queried via time travel.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Because no external `LOCATION` was specified, this is a managed table. Dropping a managed table in Databricks removes both the catalog metadata and the files stored in managed storage, so the data is deleted along with the table definition.

**Reference:** Spark SQL – *DROP TABLE* (managed tables are removed along with their data when dropped) (https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-drop-table.html)

</details>

---
## Question 186

Two common storage options on Databricks are the DBFS root and external object storage mounted via `dbutils.fs.mount`. Which statement is correct?

- **A.** DBFS exposes object storage through a filesystem interface with POSIX-like semantics.
- **B.** Only workspace admins can access DBFS root or mounts by default.
- **C.** DBFS root is more secure because mounts require public read/write permissions.
- **D.** `%sh` cells cannot read either DBFS or mounted paths.
- **E.** DBFS root is ephemeral on the driver whereas mounts always persist.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** DBFS is a filesystem abstraction layer over cloud object storage that lets you interact with blobs using familiar file-path syntax (`/dbfs/...`) and POSIX-like semantics. Mounted external storage simply exposes another object store through the same interface; both ultimately persist in cloud object storage.

**Reference:** Databricks – *DBFS overview* (https://docs.databricks.com/en/dbfs/index.html)

</details>

---
## Question 187

A legacy script was copied into a notebook:

```shell
%sh
git clone https://github.com/foo/data_loader
python data_loader/run.py
mv ./output /dbfs/mnt/new_data
```

It produces correct results but needs 20 minutes to move ~1 GB. What explains the slow performance?

- **A.** `%sh` restarts the cluster to install Git each time.
- **B.** `%sh` should be replaced with `%pip install` to parallelize execution.
- **C.** `%sh` can’t move files, so `%fs` is required for the final step.
- **D.** Python is always slower than Scala on Databricks.
- **E.** `%sh` runs exclusively on the Spark driver, so none of the worker nodes are used.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Magic command `%sh` executes entirely on the notebook’s driver node, so Git, Python, and the file copy run single-threaded on that VM and never leverage the workers or Spark. Large transfers therefore take much longer than a distributed Spark job would.

**Reference:** Databricks – *Notebook code and magic commands* (`%sh` runs only on the Spark driver) (https://docs.databricks.com/en/notebooks/notebooks-code.html)

</details>

---
## Question 188

User reviews are stored in Parquet as `(item_id, user_id, review_id, rating, review STRING)`. Data scientists want to search each review for 30 keywords. A junior engineer proposes converting the data to Delta Lake for faster filtering. How should you respond?

- **A.** Explain that Delta’s data-skipping stats track min/max values per column, which are not very selective for high-cardinality free text, so simply converting to Delta Lake won’t significantly speed up keyword scans.
- **B.** Say that Delta Lake cannot store text columns.
- **C.** Recommend running `ZORDER BY review` to see gains.
- **D.** Note that the Delta log creates inverted indexes for text automatically.
- **E.** Mention that Delta only collects statistics on the first four columns.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Delta Lake’s data-skipping relies on file-level statistics (min, max, null counts, record counts—by default only for a limited set of columns). Those stats are great for numeric columns but unhelpful when every string value is unique, so converting Parquet to Delta alone doesn’t accelerate substring searches within a free-text column.

**Reference:** Azure Databricks – *Data skipping for Delta Lake* (Delta collects min/max statistics per file to skip data; usefulness depends on column selectivity) (https://learn.microsoft.com/en-us/azure/databricks/delta/data-skipping)

</details>

---
## Question 189

Which Databricks CLI command group uploads a custom Python wheel into DBFS or other mounted object storage for production jobs?

- **A.** `databricks configure`
- **B.** `databricks fs`
- **C.** `databricks jobs`
- **D.** `databricks libraries`
- **E.** `databricks workspace`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The DBFS CLI is invoked via `databricks fs ...` (or the `dbfs` alias) and exposes commands such as `cp` to copy local files into DBFS or any mounted object storage, which is how you upload custom wheels for later use by jobs.

**Reference:** Azure Databricks – *DBFS CLI* (https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/dbfs-cli)

</details>

---
## Question 190

Analysts need a dashboard with store-level KPIs (previous-day totals, trailing-7-day averages, quarter-to-date stats). The atomic sales facts (`products_per_order`) update continuously, but analysts only need the dashboard refreshed once per day and want snappy, low-cost queries. Which solution fits?

- **A.** Run a nightly batch that aggregates the required metrics into a dedicated Delta table and overwrites it each refresh.
- **B.** Expose the streaming `products_per_order` table directly via Structured Streaming.
- **C.** Trigger a job every time a new file lands in storage.
- **D.** Keep the entire fact table pinned in Delta cache for queries.
- **E.** Point the dashboard at a raw view on `products_per_order`.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Gold-layer tables are meant to be consumption-ready, denormalized datasets tailored to a downstream project. Materializing a nightly aggregate table provides the once-a-day freshness the business wants while letting BI dashboards query a compact table quickly and cheaply instead of re-scanning the full fact stream.

**Reference:** Databricks – *Medallion architecture* (gold tables provide curated, denormalized data for reporting) (https://www.databricks.com/glossary/medallion-architecture)

</details>

---
## Question 191

An MLflow churn model produces a Spark DataFrame named `preds` with columns `customer_id`, `predictions`, and the current date. The team needs to persist each day’s batch so analysts can compare predictions over time. Which option delivers that with the least effort?

- **A.** `preds.write.mode("append").saveAsTable("churn_preds")`
- **B.** `preds.write.format("delta").save("/preds/churn_preds")`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Appending to a managed Delta table automatically tracks schema, retention, and ACID history, so each day’s results are queryable later. Writing directly to an unmanaged path provides no savings and would require extra catalog management.

**Reference:** Databricks – *Write to Delta tables from DataFrames* (https://docs.databricks.com/en/delta/delta-batch.html#append-to-a-table)

</details>

---
## Question 192

A one-terabyte JSON load must be written to Parquet with ~512 MB part-files. Auto Optimize is unavailable. Which configuration produces the desired file size without adding a shuffle?

- **A.** Set `spark.sql.files.maxPartitionBytes` to 512 MB, read the data, run the narrow transforms, and write to Parquet.
- **B.** Set `spark.sql.shuffle.partitions` to 2,048, read the data, sort it, and write to Parquet.
- **C.** Set `spark.sql.adaptive.advisoryPartitionSizeInBytes` to 512 MB, coalesce to 2,048 partitions, then write to Parquet.
- **D.** Repartition to 2,048 partitions before writing.
- **E.** Set `spark.sql.shuffle.partitions` to 512 before writing.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** `spark.sql.files.maxPartitionBytes` controls scan split size. Setting it to 512 MB yields roughly 2,048 partitions that flow through narrow transformations to the sink, producing the target file size without an extra shuffle.

**Reference:** Spark SQL performance tuning – `spark.sql.files.maxPartitionBytes` (https://spark.apache.org/docs/latest/sql-performance-tuning.html#other-configuration-options)

</details>

---
## Question 193

A streaming DataFrame `df(device_id, event_time, temp, humidity)` must compute five-minute window averages while allowing 10 minutes for late data. Which call completes the snippet?

```python
(df
  ____
  .groupBy(window("event_time", "5 minutes").alias("time"), "device_id")
  .agg(avg("temp").alias("avg_temp"), avg("humidity").alias("avg_humidity"))
  .writeStream
  .format("delta")
  .saveAsTable("sensor_avg"))
```

- **A.** `withWatermark("event_time", "10 minutes")`
- **B.** `awaitArrival("event_time", "10 minutes")`
- **C.** `await("event_time", "10 minutes")`
- **D.** `slidingWindow("event_time", "10 minutes")`
- **E.** `delayWrite("event_time", "10 minutes")`

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** You use `withWatermark` to retain state for late records. Spark drops state older than the watermark while still aggregating delayed data that arrives within 10 minutes.

**Reference:** Spark Structured Streaming – *Handling late data and watermarks* (https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)

</details>

---
## Question 194

After adding a new metric to a Structured Streaming aggregation that writes to `/item_agg` in complete mode, what else must you do before redeploying the job?

- **A.** Point `checkpointLocation` to a new path.
- **B.** Increase shuffle partitions.
- **C.** Run `REFRESH TABLE delta.'/item_agg'`.
- **D.** Register `/item_agg` with the metastore.
- **E.** Remove `.option('mergeSchema','true')`.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** The checkpoint’s metadata encodes the previous schema. Incompatible changes require starting the query with a fresh checkpoint directory so the new schema is reflected in the stored state.

**Reference:** Databricks – *Structured Streaming checkpoints* (https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/checkpoints)

</details>

---
## Question 195

A streaming job incurs extra storage cost because the default trigger fires many empty batches per minute. Records still need to be processed within 10 minutes. What adjustment meets the SLA with the lowest cost?

- **A.** Trigger every 3 seconds.
- **B.** Increase shuffle partitions.
- **C.** Trigger every 10 minutes continuously.
- **D.** Trigger every 500 milliseconds.
- **E.** Use `trigger(once=True)` and schedule the job every 10 minutes.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** `trigger(once=True)` processes available data and stops, so running it every 10 minutes satisfies the latency goal while avoiding hundreds of empty micro-batches that touch storage.

**Reference:** Databricks – *Structured Streaming triggers* (https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)

</details>

---
## Question 196

What does `pyspark.sql.functions.broadcast` do?

- **A.** Flags a low-cardinality column for partition mapping.
- **B.** Marks a column as small enough to broadcast.
- **C.** Caches a table on attached storage for every cluster.
- **D.** Marks an entire DataFrame as small enough to broadcast to all executors.
- **E.** Permanently caches a table on each node for future queries.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `broadcast(df)` hints that the full DataFrame fits in executor memory so Spark can ship it once per executor and perform a broadcast join without shuffling that dataset.

**Reference:** PySpark API – `pyspark.sql.functions.broadcast` (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.broadcast.html)

</details>

---
## Question 197

A pipeline can receive late-arriving duplicates. Beyond batch-level deduplication, how can the engineer prevent duplicates from being inserted into an existing Delta table?

- **A.** Set `delta.deduplicate = true`.
- **B.** `VACUUM` the table after each batch.
- **C.** Use an insert-only `MERGE INTO` that matches on the unique key.
- **D.** Do a full outer join and overwrite the table.
- **E.** Rely on schema enforcement.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `MERGE INTO` lets you compare new records against existing keys and only insert the ones that are not already present, preventing duplicates caused by late data.

**Reference:** Databricks – *MERGE INTO (Delta Lake upserts)* (https://docs.databricks.com/en/delta/delta-update.html#merge)

</details>

---
## Question 198

A Kafka-to-Delta stream adds new columns (`processing_time`, `topic`, `partition`) months after go-live to help diagnose latency. What limitation remains?

- **A.** Older records will not have values for the new columns.
- **B.** Spark cannot read topic or partition from Kafka.
- **C.** You cannot add columns to a production Delta table.
- **D.** The Delta log becomes invalid.
- **E.** Every new column must have a default value.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Schema evolution only affects future writes. Historical rows keep nulls for the new fields, so latency analysis can only leverage data ingested after the change.

**Reference:** Databricks – *Delta Lake schema evolution* (https://docs.databricks.com/en/delta/schema-evolution.html)

</details>

---
## Question 199

In order to facilitate near real-time workloads, a data engineer is creating a helper function to leverage the schema detection and evolution functionality of Databricks Auto Loader. The desired function will automatically detect the schema of the source directory, incrementally process JSON files, and automatically evolve the schema when new fields are detected.

The function is displayed below with a blank:

```python
def auto_load_json(source_path: str, checkpoint_path: str, target_table_path: str):
  return (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(source_path)
    ___________
  )
```

Which response correctly fills in the blank to meet the specified requirements?

- **A.** `.write.option("path", target_table_path).mode("append")`
- **B.** `.writeStream.option("path", target_table_path).start()`
- **C.** `.write.format("delta").save(target_table_path)`
- **D.** `.writeStream.option("checkpointLocation", checkpoint_path).option("mergeSchema", "true").table(target_table_path)`
- **E.** `.writeStream.trigger(once=True).format("json").start(target_table_path)`

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Auto Loader must write with `writeStream` and maintain a `checkpointLocation` for recoverability. Enabling `mergeSchema` allows the Delta table to evolve automatically as new columns arrive. The other options either use batch writers, omit the checkpoint, or write raw JSON output without schema evolution.

**Reference:** Databricks – *Auto Loader schema inference and evolution* (https://docs.databricks.com/en/ingestion/auto-loader/schema.html)

</details>

---
## Question 200

```python
import pyspark.sql.functions as F

(spark.table("silver_customer_sales")
  .groupBy("customer_id")
  .agg(F.min("sale_date").alias("first_transaction_date"),
       F.max("sale_date").alias("last_transaction_date"),
       F.mean("sale_total").alias("average_sales"),
       F.countDistinct("order_id").alias("total_orders"),
       F.sum("sale_total").alias("lifetime_value"))
  .write
  .mode("overwrite")
  .table("gold_customer_lifetime_sales_summary"))
```

Assuming the aggregates are correct, what does this code do?

- **A.** Overwrite `silver_customer_sales` with data from the gold table.
- **B.** Incrementally update only changed rows in the gold table.
- **C.** Overwrite `gold_customer_lifetime_sales_summary` with aggregates computed from all rows in `silver_customer_sales`.
- **D.** Use a state store to update aggregates incrementally.
- **E.** Detect new rows in silver and overwrite only when the table changes.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** `mode("overwrite").table(...)` replaces the target table with the newly computed batch. No incremental processing or MERGE logic occurs; every run recomputes the full aggregate.

**Reference:** Databricks – *Overwrite Delta tables from DataFrames* (https://docs.databricks.com/en/delta/delta-batch.html#overwrite-data)

</details>

---
## Question 201

To enforce that every table is external (unmanaged) Delta, what should engineers do when writing each table?

- **A.** Specify `LOCATION` only when creating databases.
- **B.** Store all data in an external data warehouse.
- **C.** Save each table by specifying an explicit filesystem path (for example, `df.write.format("delta").save(/mnt/... )`).
- **D.** Use the `EXTERNAL` keyword in `CREATE TABLE` statements.
- **E.** Mount cloud storage before creating the workspace.

<details><summary>Answer</summary>

**Answer:** C

**Explanation:** Delta tables written to an explicit path (`save`/`CREATE TABLE ... LOCATION 'path'`) are unmanaged—data lives exactly where specified and Databricks only tracks metadata. Managed tables are created when no path is provided.

**Reference:** Databricks – *Managed and unmanaged Delta tables* (https://docs.databricks.com/en/delta/manage-tables/index.html#managed-and-unmanaged-tables)

</details>

---
## Question 202

A nightly job reads all files for the previous day, calls `dropDuplicates(["customer_id", "order_id"])`, and writes the batch to the `orders` table. The upstream system can emit duplicate orders hours apart. What’s true of the table after this job runs?

- **A.** Only new unique rows are written and duplicates already in the table are removed.
- **B.** Each batch contains unique rows, but duplicates that already exist in the table remain.
- **C.** Existing duplicates are overwritten automatically.
- **D.** The write fails if matching keys already exist.
- **E.** The write deduplicates the union of historical and new rows.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** `dropDuplicates` only removes duplicates within the DataFrame being written. It does not compare against existing table contents, so previously written duplicates remain untouched.

**Reference:** PySpark API – `DataFrame.dropDuplicates` (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.dropDuplicates.html)

</details>

---
## Question 203

Marketing needs to share an aggregate table with Sales, but some columns should be hidden and column names should follow Sales conventions. What is the simplest solution?

- **A.** Create a view that selects/renames the approved columns for Sales.
- **B.** Create a new table via DEEP CLONE.
- **C.** Use CTAS to build a derivative table and schedule refreshes.
- **D.** Update the ETL to write a second Sales-specific table.
- **E.** Export CSVs and email them.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** A view can filter out sensitive columns and alias field names without duplicating storage or changing the upstream pipeline, which is the simplest way to present the data differently to another audience.

**Reference:** Databricks SQL – *Use views to control access* (https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-view.html)

</details>

---
## Question 204

A Delta table has a check constraint enforcing valid latitude/longitude ranges. What happens when an insert batch includes a row with longitude `212.67`?

- **A.** Spark writes valid rows and skips the invalid row.
- **B.** The entire transaction fails and nothing is written.
- **C.** The bad record is quarantined in a separate table.
- **D.** The row is written but a boolean flag is set.
- **E.** The row is written and a warning is logged.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Check constraints are enforced atomically; a violation aborts the transaction and no records are committed.

**Reference:** Databricks – *Delta constraints* (https://docs.databricks.com/en/delta/delta-constraints.html)

</details>

---
## Question 205

A user tries to change a job’s owner to the `DevOps` group but cannot. Why?

- **A.** Jobs can only have a single user owner; groups cannot be owners.
- **B.** The creator is permanently the owner.
- **D.** Users must belong to a group to assign ownership to it.
- **E.** Only workspace admins can set group ownership.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Databricks Jobs support exactly one owner, and ownership can only be assigned to an individual user—not to a group.

**Reference:** Databricks – *Job permissions* (https://docs.databricks.com/en/workflows/jobs/jobs.html#job-permissions)

</details>

---
## Question 206

A Delta table stores Kafka records from five topics, but only the `registration` topic contains PII that must be access-controlled and retained for only 14 days. What design meets both requirements?

- **A.** Delete all records biweekly and rely on time travel.
- **B.** Partition by a `registration` flag.
- **C.** Assume binary values are not PII.
- **D.** Use different storage containers per partition.
- **E.** Partition the table by `topic` so ACLs and deletes can target the `registration` partition independently.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Partitioning by `topic` isolates `registration` data into its own directory, allowing you to apply table ACLs and run targeted deletes/VACUUM for that partition without affecting other topics.

**Reference:** Databricks – *Partition Delta Lake tables* (https://docs.databricks.com/en/delta/optimizations/optimize.html#partitioning)

</details>

---
## Question 207

A group is granted `USAGE` and `SELECT` on database `prod`. They are not admins. What can they do?

- **A.** Administer all permissions.
- **B.** List tables but not read data.
- **C.** Modify data but not create tables.
- **D.** Query objects but cannot create or change them.
- **E.** Create tables but not UDFs.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** `USAGE` + `SELECT` grants read access to the database but no ability to create, drop, or modify objects.

**Reference:** Databricks – *Table ACL privileges* (https://docs.databricks.com/en/security/access-control/table-acls/table-acl-overview.html)

</details>

---
## Question 208

Where can an admin view the timeline of cluster resize events to diagnose autoscaling?

- **A.** Workspace audit logs
- **B.** Driver log
- **C.** Ganglia
- **D.** Cluster Event Log
- **E.** Executor log

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** The Cluster Event Log records scaling events, driver changes, and node additions/removals with timestamps.

**Reference:** Databricks – *Cluster event log* (https://learn.microsoft.com/en-us/azure/databricks/compute/clusters/cluster-event-log)

</details>

---
## Question 209

While reviewing Ganglia metrics for a three-node cluster, which sign shows the VMs are well utilized?

- **A.** Flat five-minute load average
- **B.** Bytes received < 80 MB/s
- **C.** No network spikes
- **D.** Constant disk space
- **E.** CPU utilization around 75%

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** Balanced CPU usage (roughly 70–80%) indicates the nodes are neither idle nor pegged, which is the goal when sizing clusters.

**Reference:** Databricks – *Monitor clusters with Ganglia* (https://learn.microsoft.com/en-us/azure/databricks/administration-guide/monitoring/ganglia)

</details>

---
## Question 210

Which technology helps highlight important text patterns in Spark driver log4j output?

- **A.** Regular expressions
- **B.** Julia
- **C.** `pyspark.ml.feature`
- **D.** Scala Datasets
- **E.** C++

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Regex is ideal for searching log text for signatures such as stack traces or error codes.

**Reference:** Python docs – `re` module (https://docs.python.org/3/library/re.html)

</details>

---
## Question 211

You run `assert(myintegrate(lambda x: x*x, 0, 3)[0] == 9)`. What kind of test is this?

- **A.** Unit
- **B.** Manual
- **C.** Functional
- **D.** Integration
- **E.** End-to-end

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** The assertion validates a single function (`myintegrate`) in isolation, which is the definition of a unit test.

**Reference:** Microsoft Learn – *Unit testing best practices* (https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices)

</details>

---
## Question 212

A Databricks job contains three notebook tasks. Task A has no dependencies. Tasks B and C each depend on task A and otherwise run in parallel. During a scheduled run, task A fails. What describes the resulting state?

- **A.** Because tasks form a dependency graph, no work is committed until the entire job succeeds.
- **B.** Tasks B and C still run; all work produced by task A is rolled back automatically.
- **C.** The job engine rolls back every task because the DAG did not finish successfully.
- **D.** Tasks B and C are skipped; any work already committed by task A remains because Databricks does not automatically roll back notebook logic.
- **E.** Tasks B and C are skipped and task A's work is also rolled back due to stage failure.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** When a task fails, downstream tasks in the DAG are skipped, but Databricks does not provide cross-task transactions—any statements that A committed before failing remain in the Lakehouse.

**Reference:** Databricks – *Jobs with multiple tasks* (https://docs.databricks.com/en/workflows/jobs/jobs.html#multi-task-jobs)

</details>

---
## Question 213

A junior engineer wants to mix Python and SQL in one notebook to create a filtered view. The workspace only contains the `geo_lookup` and `sales` tables. They run the following cells: 

```python
Cmd 1 (%python)
countries_af = [x[0] for x in (
    spark.table("geo_lookup")
         .filter("continent = 'AF'")
         .select("country")
         .collect()
)]
```

```sql
Cmd 2 (%sql)
CREATE VIEW sales_af AS
  SELECT *
  FROM sales
  WHERE city IN countries_af
    AND continent = 'AF';
```

What happens when the cells are executed in order?

- **A.** Both commands succeed; `countries_af` and `sales_af` appear as views.
- **B.** Cmd 1 succeeds, and Cmd 2 resolves `countries_af` as a table or view if it exists.
- **C.** Cmd 1 succeeds and Cmd 2 fails because `countries_af` is a PySpark DataFrame.
- **D.** Both commands fail because cross-language execution is not supported.
- **E.** Cmd 1 succeeds and Cmd 2 fails because `countries_af` is a Python list that SQL cannot reference.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** The `%python` cell materializes `countries_af` as a standard Python list after collecting the query results. SQL cells cannot read Python variables, so the CREATE VIEW statement fails with `TABLE OR VIEW NOT FOUND: countries_af`.

**Reference:** Databricks – *Pass values between languages in notebooks* (https://docs.databricks.com/en/notebooks/notebook-workflows.html#pass-values-between-languages)

</details>

---
## Question 214

Which statement about Spark configuration on Databricks is true?

- **A.** You can change Spark configs for a running interactive cluster via the REST API without restarting it.
- **B.** A config set inside one notebook automatically applies to every notebook attached to the cluster.
- **C.** Spark configs for interactive clusters can only be set with a global init script.
- **D.** Spark configs set in the cluster configuration UI apply to every notebook attached to that cluster.
- **E.** Notebook-level configs are ignored whenever the same key is defined at the cluster level.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Cluster-level Spark configuration set through the Clusters UI becomes part of the SparkSession for every notebook attached to that cluster. Notebook-level configs apply only to that session but do not override UI settings already in effect.

**Reference:** Databricks – *Spark configuration* (https://docs.databricks.com/en/compute/configure.html#spark-configuration)

</details>

---
## Question 215

A developer cloned a repo with Databricks Repos but cannot push directly to `main`. How can they share updates without risking teammates’ work?

- **A.** Export a git diff and email it to the team.
- **B.** Fork the repo and work outside Databricks.
- **C.** Pull upstream changes and push to whatever branch appears automatically.
- **D.** Merge local changes into `main` and force push.
- **E.** Create a new branch in Repos, commit changes there, and push the branch to the remote so a pull request can be opened.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** The recommended workflow is to branch from the protected branch, commit in that branch, and push it to the remote so the team can review and merge via a pull request—keeping `main` safe.

**Reference:** Databricks – *Work with branches in Repos* (https://docs.databricks.com/en/repos/index.html#work-with-branches)

</details>

---
## Question 216

Developers use `SHALLOW CLONE` to copy production Delta tables into a dev workspace. After the production tables are vacuumed, several cloned tables stop working. Why?

- **A.** Type 1 updates invalidate clones automatically.
- **B.** Running `VACUUM` deletes shallow clones; use `DEEP CLONE` instead.
- **C.** Shallow clones expire after seven days by design.
- **D.** Shallow clones reference the source table’s data files; `VACUUM` removed those files because they were no longer needed, leaving the clones without data.
- **E.** The compaction job rewrote files and `REFRESH TABLE` must be run.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** A shallow clone stores only metadata pointers to the original data files. When `VACUUM` removes old files, any clone that still references them becomes unusable.

**Reference:** Databricks – *Clone Delta tables* (https://docs.databricks.com/en/delta/clone.html#shallow-clone)

</details>

---
## Question 217

You need to join a streaming DataFrame `streamingDF` with a static lookup table `userLookup`. Which code attempts an invalid stream-static join?

- **A.** `userLookup.join(streamingDF, ["user_id"], how="inner")`
- **B.** `streamingDF.join(userLookup, ["user_id"], how="outer")`
- **C.** `streamingDF.join(userLookup, ["user_id"], how="left")`
- **D.** `streamingDF.join(userLookup, ["user_id"], how="inner")`
- **E.** `userLookup.join(streamingDF, ["user_id"], how="right")`

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** Structured Streaming supports inner joins between a stream and static table, plus left outer/semi/anti joins when the streaming DataFrame is on the left. Full outer joins are unsupported for stream-static combinations.

**Reference:** Databricks – *Streaming joins* (https://docs.databricks.com/en/structured-streaming/joins.html)

</details>

---
## Question 218

Where can you see primary indicators that a partition is spilling to disk?

- **A.** Query detail screen and Job detail screen
- **B.** Stage detail screen and Executor log files
- **C.** Driver and Executor log files
- **D.** Executor detail screen and Executor log files
- **E.** Stage detail screen and Query detail screen

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** The Spark UI’s Stage details show shuffle read/write sizes per task, and executor logs capture the “Spilling” warnings. Together they are the quickest way to diagnose spill.

**Reference:** Databricks – *Monitor and debug Spark jobs* (https://docs.databricks.com/en/compute/cluster-monitor.html)

</details>

---
## Question 219

A job ingests hourly Parquet drops with the following logic:

```python
(spark.readStream
  .format("parquet")
  .load("/mnt/raw_orders/")
  .withWatermark("time", "2 hours")
  .dropDuplicates(["customer_id", "order_id"])
  .writeStream
  .trigger(once=True)
  .table("orders"))
```

If the source occasionally enqueues duplicate orders several hours apart, what happens?

- **A.** Duplicates arriving more than two hours apart can persist in the `orders` table.
- **B.** Every record waits two hours in state before being written.
- **C.** Only the most recent two hours of records remain in the table.
- **D.** Duplicates in the same batch may survive but late duplicates are dropped.
- **E.** Late records (>2 hours) are ignored entirely, so no duplicates ever appear.

<details><summary>Answer</summary>

**Answer:** A

**Explanation:** Watermarking lets Structured Streaming drop state for keys older than two hours; duplicates that arrive after the watermark horizon are treated as brand-new rows and are not removed.

**Reference:** Databricks – *Deduplicate with watermarks* (https://docs.databricks.com/en/structured-streaming/deduplication.html)

</details>

---
## Question 220

A star-schema workload is being migrated from an RDBMS to Delta Lake. Which consideration influences the migration design?

- **A.** Delta Lake requires hashed foreign keys.
- **B.** All SQL can be copied verbatim because Databricks enforces the same constraints.
- **C.** Multi-table commits require explicit locks to avoid deadlocks.
- **D.** Delta transactions are ACID per table and Delta Lake only enforces NOT NULL and CHECK constraints—foreign keys are not enforced.
- **E.** Foreign keys must reference primary keys via MERGE statements.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Delta Lake provides ACID guarantees at the table level but only enforces NOT NULL and CHECK constraints, so foreign key relationships and multi-table transactions must be handled in application logic.

**Reference:** Databricks – *Delta Lake constraints* (https://docs.databricks.com/en/delta/delta-constraints.html)

</details>

---
## Question 221

To audit all street-address changes, an architect proposes a Type 1 table and relying on Delta time travel. A data engineer recommends Type 2. What fact drives the decision?

- **A.** Type 2 updates corrupt data if a query fails mid-way.
- **B.** Shallow clones accelerate historic queries.
- **C.** Time travel cannot query previous versions of Type 1 tables.
- **D.** Long-term time travel retention increases storage and query cost because old data files must be preserved until VACUUM removes them; Type 2 tables explicitly model history without relying on indefinite retention.
- **E.** Delta Lake only supports Type 0 tables.

<details><summary>Answer</summary>

**Answer:** D

**Explanation:** Time travel works only while Delta retains historical files, which grows storage costs and slows queries if you keep versions indefinitely. Modeling history in a Type 2 table provides explicit, queryable history without relying on unbounded retention.

**Reference:** Databricks – *Delta Lake time travel and retention* (https://docs.databricks.com/en/delta/tune-performance/delta-optimizations.html#time-travel-and-data-retention)

</details>

---
## Question 222

Given the view definition:

```sql
CREATE VIEW user_ltv_no_minors AS
SELECT email, age, ltv
FROM user_ltv
WHERE CASE WHEN is_member('auditing') THEN TRUE ELSE age >= 18 END;
```

What does a user who is not in the `auditing` group see when querying the view?

- **A.** All rows, but ages under 18 are masked.
- **B.** Ages under 18 appear as NULL.
- **C.** The `age` column is entirely NULL.
- **D.** Every row from `user_ltv`.
- **E.** Only rows where `age >= 18`; minors are filtered out.

<details><summary>Answer</summary>

**Answer:** E

**Explanation:** `is_member` evaluates to FALSE for the user, so the CASE expression returns the predicate `age >= 18`. The view therefore filters out minors for non-auditors.

**Reference:** Databricks SQL – `is_member` function (https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/is_member)

</details>

---
## Question 223

The GDPR delete pipeline removes IDs from `user_lookup` and then deletes matching rows from `user_aggregates`. After the DELETE finishes, are the records guaranteed to be inaccessible?

- **A.** Yes; DELETE permanently removes data without extra steps.
- **B.** No; until `VACUUM` removes the invalidated files, time travel can still access the deleted data.
- **C.** Yes; change data feed enforces cascading deletes.
- **D.** No; DELETE only works inside MERGE statements.
- **E.** No; change data feed does not track deletes.

<details><summary>Answer</summary>

**Answer:** B

**Explanation:** DELETE marks the files containing the rows as removed, but Delta retains those files for the configured retention period for time travel. You must run `VACUUM` after the retention period to physically remove them.

**Reference:** Databricks – *VACUUM and data retention* (https://docs.databricks.com/en/delta/tune-performance/vacuum.html)

</details>

---
