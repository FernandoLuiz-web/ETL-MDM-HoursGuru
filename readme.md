# ETL Pipeline for Clockify and Dataverse

This project implements an ETL (Extract, Transform, Load) pipeline to process data from two sources: **Clockify** and **Dataverse**. The extracted data is transformed and loaded into a **MongoDB** database. The pipeline is orchestrated using **Prefect**, a modern workflow orchestration framework.

## Overview

The ETL pipeline consists of two main flows:

1. **Clockify Flow**: Extracts time tracking data (appointments) from Clockify, transforms it, and loads it into MongoDB.
2. **Dataverse Flow**: Extracts project planning data from Dataverse, transforms it, and loads it into MongoDB.

Both flows are executed concurrently using Python's `ThreadPoolExecutor`.

## Features

- **Data Extraction**:
  - **Clockify**: Fetches active projects and their time entries for a specific date range.
  - **Dataverse**: Fetches project planning data from a REST API.
- **Data Transformation**:
  - Normalizes and validates the extracted data using **Pandas** and **Pandera**.
- **Data Loading**:
  - Inserts the transformed data into MongoDB collections.
- **Orchestration**:
  - Uses **Prefect** to manage and monitor the ETL tasks.

## How It Works

### Clockify Flow

1. **Extract**:
   - Fetches active projects from Clockify.
   - Retrieves time entries for each project within a specific date range.
2. **Transform**:
   - Normalizes the data structure.
   - Converts timestamps to the appropriate timezone.
   - Renames columns for consistency.
3. **Load**:
   - Validates the transformed data schema.
   - Inserts new records into the `AppointedHours` collection in MongoDB.

### Dataverse Flow

1. **Extract**:
   - Fetches project planning data from the Dataverse API.
2. **Transform**:
   - Converts date fields to a standard format.
   - Filters and renames columns for consistency.
3. **Load**:
   - Validates the transformed data schema.
   - Inserts new records into the `ProjectPlanning` collection in MongoDB.

### Orchestration

- **Prefect** is used to define and manage the ETL tasks.
- Each flow is executed as a Prefect flow with a `ConcurrentTaskRunner` for parallel task execution.
- The flows are triggered concurrently using `ThreadPoolExecutor` in the `main.py` file.

## Prerequisites

- Python 3.9 or higher
- MongoDB instance
- Clockify API key
- Dataverse API key

## Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>