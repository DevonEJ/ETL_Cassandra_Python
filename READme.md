# Operating Instructions

---

### Summary

<p>
    This project is an ETL pipeline for Sparkify, extracting historic event date from their music streaming application, transforming it into a required denormalised format, and then inserting into 3 tables in the sparkifydb Cassandra keyspace.
    
    
</p>

### To run the ETL pipeline and execute queries

<p>
    
1. In the terminal, run <code> python etl.py </code> 
    to create and populate the required tables from the CSV files stored in event_data.
    
2. Use the notebook, <code> run_queries_here.ipynb</code> to execute queries on the populated tables.
    
3. To edit any sql queries, please see <code>cql_queries.py</code> to edit CQL queries, and <code>utils.py</code> to edit the transformation of the data, and/or the data sources used.
    
</p>