# Analytics Engineer Challenge – End-to-End Setup & Execution Guide

## 📌 Overview

This project demonstrates an end-to-end analytics workflow including:

* Data ingestion from CSV
* Dimensional modeling using **dbt**
* Incremental data processing
* Python-based data exports
* Visualization in Power BI

---

## 🧱 Project Structure

```
project/
├── data/                      # Raw CSV input
├── dbt_project
│    ├── models/
│    │      ├── staging/              # stg models
│    │      ├── intermediate/         # int models
│    │      └── marts/                # fact & dimension tables
│    ├── macros/                   # custom macros (e.g. schema naming)
│    └── dbt_project.yml
├── notebooks/                # Jupyter notebook (Python tasks)
├── .env                      # environment variables (not committed)
├── .env.example              # template for env vars
├── profiles.yml              # dbt connection config
└── README.md
```

---

## ⚙️ 1. Environment Setup

### 🔹 1.1 Create virtual environment (Python version installed 3.9.16)

```bash
python -m venv venv
```

Activate:

* Windows:

```bash
venv\Scripts\activate
```

* Mac/Linux:

```bash
source venv/bin/activate
```

---

### 🔹 1.2 Install dependencies

```bash
pip install -r requirements.txt
```

---

### 🔹 1.3 Download Docker Desktop


---

## 🔐 2. Environment Variables

Create a `.env` file:

```
.env.example has been provided -> rename to .env
```

Load it in Python when needed.

---

## 🛢️ 3. Database Setup (PostgreSQL)

Download and configure PostgreSQL version 14.22 with:

* Host: `localhost`
* Port: `5432`
* Database: `challenge`
* User: `postgres`
* Password: `postgres`

---

## 🔌 4. Configure dbt Profile

Edit `profiles.yml`:

profiles.yml has been properly configured to work with .env, can be changed if needed

---

## 📥 5. Data Ingestion + Docker initialization (Jupyter)

* Configure jupyter notebook to use the venv

Run the first 4 notebook cells:

* Launches docker with postgre and sftp:

* Loads CSV into:

```
raw_freight_loads
```

* Uses:

  * **replace**
  * creates table if not exists/ replaces if exists

* Loads environment variables:
---

## 🔄 6. Install dbt Packages

* Run the next cell

---

## 🏗️ 7. Run and test dbt Models

### Run all models:


* Run the next cell
    * You will notice test failures, generally they need to be corrected either by contacting source owner or applying custom logic. Here I opted to ignore them as there was no clear direction.
* Run the next cell ignoring the failing tests

---

## 🧠 Data Modeling Approach

### Layers:

| Layer        | Description                 |
| ------------ | --------------------------- |
| raw          | ingested CSV                |
| staging      | incremental, cleaned data   |
| intermediate | transformations             |
| marts        | fact and dimension tables   |

---

### Key Features:

* Incremental staging model
* Surrogate key using `dbt_utils`
* Late-arriving data handling
* Fact table: `fct_loads`
* Dimensions:

  * `dim_carrier_tb`
  * `dim_location_tb`

---

## 🐍 8. Python Tasks (Jupyter Notebook)

### Implemented functions:

* Split `lane` was implemented in dbt to allow for direct connect to database when possible.

* May run all the remaining cells including (docker stop) to:

    * Send CSV via:

        * email
        * sFTP

    * Export final dataset
        * Note that for my Power BI locale I had to apply Decimal separator conversion 412.32 → 412,32. This might not be needed for you deppending on the Power BI locale you have.

---

## 📊 9. Power BI

* Open freights.pbix in "power bi" folder via Power BI Desktop

---

## 🧪 10. Notes & Assumptions

* PostgreSQL version: 14.22

  * uses `delete+insert` instead of `merge`
* Surrogate keys used for change detection
* Incremental logic based on `source_date`
* Partition and indexing wasn't used due to small data size/limited use
---

## 🏁 Final Output

* Dimensional model in PostgreSQL
* Exported CSV file
* Power BI dashboard

---
