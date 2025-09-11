# dataops-pipeline-demo
🚀 Data Pipeline (Azure + Databricks + Delta Lake)

This project implements a data ingestion and transformation pipeline using Apache Spark on Databricks and Azure Data Lake (Blob Storage), following the Bronze → Silver → (Gold) medallion architecture.

📂 Architecture

The pipeline is designed according to the Delta Lake medallion architecture:

Bronze: raw data ingested directly from external sources (CSV, JSON, APIs, databases, etc.).

Silver: cleaned and enriched data prepared for analysis or intermediate consumption.

Gold: (future scope) curated and optimized data for reporting, analytics, or business applications.

⚙️ Technologies

Databricks (remote cluster connection via Databricks Connect)

Apache Spark for distributed data processing

Azure Blob Storage as Data Lake

Delta Lake for transactional storage

Python (PySpark + Pandas) for data transformations

📒 Notebooks

The notebooks included in this project cover the following tasks:

Ingestion (Bronze)

Read data from multiple external sources.

Store datasets in Azure Blob Storage (Bronze container) in Delta format.

Transformation (Silver)

Handle missing values and normalize data.

Apply column transformations (e.g., text parsing, splitting identifiers into prefix/number, etc.).

Store transformed datasets in Azure Blob Storage (Silver container) in Delta format.

Exploration and analysis

Read Delta tables from the Silver layer.

Perform exploratory operations: joins, filters, aggregations, window functions, etc.

🔑 Environment Variables

Sensitive credentials and configuration values (e.g., access keys to Azure Blob Storage) are not hardcoded but managed as environment variables within the Databricks cluster:

fs.azure.account.key.<storage_account>.blob.core.windows.net

storage_account_name

container_name

This ensures security and portability across environments.

📈 Next Steps

Implement the Gold layer with curated datasets for advanced analytics.

Automate the pipeline with Azure Data Factory or Databricks Workflows.

Expose Delta tables as SQL Endpoints for BI tools.