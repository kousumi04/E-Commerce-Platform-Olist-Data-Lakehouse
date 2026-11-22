# RetailPulse — System Architecture

## Overview
RetailPulse is a local-first, cloud-backed e-commerce data lakehouse built on the Brazilian Olist E-Commerce dataset. It uses local PySpark for zero-cost distributed ETL, Amazon S3 for 3-tier Medallion storage, Amazon Athena for serverless SQL analytics, and Power BI Desktop (Import Mode) for interactive BI.

## Data Flow
```mermaid
flowchart TD
    A[Kaggle Olist CSVs<br/>9 Files, ~120 MB] --> B[Boto3 Ingestion]
    B --> C[(Amazon S3: raw/<br/>Immutable CSVs)]
    A --> D[Local PySpark ETL<br/>Clean, Cast, Validate]
    D --> E[(Amazon S3: processed/<br/>Cleaned Parquet)]
    D --> F[Dimensional Modeling<br/>4 Facts + 5 Dimensions]
    F --> G[(Amazon S3: curated/<br/>Star Schema Parquet)]
    G --> H[Amazon Athena<br/>Manual External Tables]
    H --> I[Power BI Desktop<br/>Import Mode]
    I --> J1[Executive Sales]
    I --> J2[Product Intelligence]
    I --> J3[Customer Intelligence]
    I --> J4[Logistics Performance]