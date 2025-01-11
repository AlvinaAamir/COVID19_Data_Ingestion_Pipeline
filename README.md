# COVID-19 Data Ingestion Pipeline

## Objective
This project demonstrates the creation of a Python-based data ingestion pipeline to fetch COVID-19 case data, clean and preprocess it, and store it in a Microsoft SQL Server database.

---

## Pipeline Workflow
1. **Data Source**:
   - API: [Ontario Data API](https://data.ontario.ca/)
   - Parameters:
     - `resource_id`: The unique identifier of the dataset.
     - `limit`: Number of records to fetch.
   - Authentication: None required; public API.

2. **Steps**:
   - **Data Extraction**:
     - API call using Python's `requests` library.
   - **Data Cleaning**:
     - Convert date fields to datetime.
     - Handle missing values and replace `Outcome1` nulls with "NON FATAL."
     - One-hot encoding for gender and creation of age categories.
   - **Feature Engineering**:
     - Extract year and month from reported dates.
     - Create a binary indicator for fatal outcomes.
   - **Data Ingestion**:
     - Store the cleaned data in a Microsoft SQL Server database.

---

## Tools & Libraries
- **Python Libraries**:
  - `requests`
  - `pandas`
  - `sqlalchemy`
- **Database**:
  - Microsoft SQL Server

---

## Files
1. **Jupyter Notebook**: [Group A2-1.ipynb](./Group%20A2-1.ipynb)
2. **Python Script**: [Group Assignment 2-1.py](./Group%20Assignment%202-1.py)

---

## How to Run
1. Clone this repository:
   ```bash
   git clone https://github.com/YourUsername/COVID19_Data_Ingestion_Pipeline.git

