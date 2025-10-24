# FPL Lakehouse Predictor

A data engineering project to build a medallion architecture lakehouse to store FPL data.

Ultimate goal to predict the optimal Fantasy Premier League (FPL) team for the next five gameweeks using ML.

This project demonstrates the full data lifecycle - from ingestion and transformation to machine learning and visualisation. It is designed for blogs, conference talks, and case studies to showcase the power of modern data platforms.

---

## Project Objectives

- Ingest and process FPL data from public APIs
- Apply ETL using PySpark and Delta Lake
- Build a dimensional model using Kimball-style design
- Engineer features and train predictive models
- Optimise team selection under FPL constraints
- Visualise insights using Power BI
- Implement DataOps practices including:
  - Modular codebase
  - Dev/Test/Prod environments
  - Unit testing with `pytest`
  - CI/CD with GitHub Actions
  - Strong governance and documentation

---

## Architecture Overview

### Lakehouse Layers
- **Bronze**: Raw ingested data from FPL API
- **Silver**: Cleaned and conformed datasets
- **Gold**: Feature-rich tables for ML and reporting

### Tech Stack
- Databricks (Free Edition)
- Fabric
- Delta Lake
- Polars
- PySpark
- MLflow
- Power BI
- GitHub + VS Code