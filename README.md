# Airflow Repository  

This repository demonstrates key concepts and best practices in **Apache Airflow** for building, orchestrating, and monitoring data pipelines.  

---

## 📌 Covered Concepts  

### 1. **DAG and Task Decorators**  
- Define DAGs and tasks using `@dag` and `@task` for cleaner, Pythonic DAGs.  

### 2. **Task Branching**  
- Run different downstream tasks based on conditions (e.g., data validation pass/fail).  

### 3. **Task Grouping**  
- Organize related tasks with `TaskGroup` for better readability in complex DAGs.  

### 4. **XComs**  
- Share data between tasks using **XCom** (e.g., API response → transformation).  

### 5. **Assets**  
- Treat external datasets/systems as **assets** for lineage tracking and asset-based scheduling.  

### 6. **API Retry & Error Handling**  
- Built-in retry logic with delays for API calls to handle transient failures gracefully.  

### 7. **Data Quality Checks**  
- Integrated with **Great Expectations** for validation.  
- Example: Ensure `id` is not null and `email` has valid format.  

### 8. **Data Quality Branching**  
- If checks pass → insert into main table.  
- If checks fail → insert into a **data quality rejects table** for review.  
