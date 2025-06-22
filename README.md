<<<<<<< HEAD
Sales Analytics Dashboard & ETL PipelineTable of ContentsProject OverviewFeaturesArchitectureTechnologies UsedSetup and RunningSQL ScriptsSample OutputsFuture EnhancementsLicenseProject OverviewThis project provides a complete solution for sales data analytics, encompassing an Extract, Transform, Load (ETL) pipeline and a dynamic web-based dashboard. The ETL process, built with PySpark, ingests raw sales data, cleans it, performs key aggregations, and stores the results in an SQLite database and Parquet files. A Flask API then serves this processed data to a responsive front-end dashboard, allowing users to visualize sales trends, profitability, customer insights, and discount performance.FeaturesData Ingestion: Automatically downloads raw sales data from Kaggle.Data Cleaning & Validation: Handles missing values, data type casting, and removes duplicate records.Key Performance Indicators (KPIs): Calculates and displays total sales, total profit, average discount, and total orders.Monthly Sales Trend Analysis: Visualizes sales performance over time, with filtering capabilities by country.Top Customer Identification: Lists and charts the top 10 most profitable customers.Product Line Profitability: Displays total profit across different product categories.Discount Analysis: Shows average discount rates per product line.High-Value Order Tracking: Lists individual orders exceeding a defined sales threshold.Flexible Data Storage: Stores processed data in a local SQLite database (for API consumption) and partitioned Parquet files (for scalable storage and further analysis).Responsive Web Dashboard: A clean, intuitive, and responsive UI built with HTML, Tailwind CSS, and Chart.js.ArchitectureThe project follows a typical data analytics architecture, separating the data processing (ETL) from the data presentation (API & Dashboard).+----------------+      +----------------+      +------------------+      +-----------------+
| Raw Sales Data |----->| Data Pipeline  |----->| Processed Data   |----->| Flask API       |
| (Kaggle CSV)   |      | (PySpark ETL)  |      | (SQLite, Parquet)|      | (Data Endpoints)|
+----------------+      +----------------+      +------------------+      +--------+--------+
                                                                                    |
                                                                                    | (JSON)
                                                                                    v
                                                                          +-------------------+
                                                                          | Web Dashboard     |
                                                                          | (HTML, JS, CSS)   |
                                                                          +-------------------+
Components:data_pipeline.py (PySpark ETL):Ingestion: Fetches sales_data_sample.csv from a specified Kaggle dataset using kagglehub.Cleaning & Validation: Converts data types, handles nulls (fills numeric with 0, strings with "N/A"), removes duplicate order lines, and filters out invalid records (e.g., negative sales).Enrichment: Calculates DISCOUNT (if not present) and Profit based on an assumed PROFIT_MARGIN_RATE.Aggregation: Generates summary tables like monthly_sales_by_region, top_10_customers_by_profit, category_wise_avg_discount.Reporting Queries: Executes Spark SQL queries for sales_above_threshold, profitable_categories, and customers_high_discount.Storage: Stores all aggregated and queried results into:sales_analytics.db (SQLite database) for the API.output/parquet/ (Partitioned Parquet files) for scalable data warehousing.app.py (Flask API):A lightweight Flask application that serves as the backend for the dashboard.Connects to the sales_analytics.db SQLite database.Exposes several RESTful API endpoints (/api/kpis, /api/monthly_sales, etc.) to retrieve processed sales data in JSON format.Supports filtering for monthly sales data by country.index.html (Web Dashboard):The single-page front-end application.Built with HTML, styled with Tailwind CSS for responsiveness and modern aesthetics.Uses Chart.js to render interactive data visualizations (line charts, bar charts).Fetches data from the Flask API using JavaScript (fetch API) and dynamically updates the dashboard elements.Technologies UsedPython 3.xApache Spark (PySpark): For large-scale data processing and ETL.Pandas: Used within PySpark for efficient data handling and conversion.Flask: A micro web framework for building the RESTful API.Flask-CORS: Enables Cross-Origin Resource Sharing for the API.SQLite: A lightweight, file-based database used for API data storage.KaggleHub: For easy ingestion of datasets from Kaggle.HTML5Tailwind CSS: A utility-first CSS framework for responsive and fast styling.Chart.js: A JavaScript charting library for creating interactive data visualizations.Setup and RunningFollow these steps to set up and run the Sales Analytics Dashboard locally.PrerequisitesPython 3.x: Make sure Python is installed on your system.Java Development Kit (JDK): Spark requires a compatible JDK.Apache Spark: Download and set up Apache Spark. Ensure SPARK_HOME and PATH environment variables are configured correctly to include Spark's bin directory.You can verify your Spark setup by running spark-shell or pyspark in your terminal.Git: For cloning the repository.1. Clone the Repositorygit clone https://github.com/your-username/sales-analytics-dashboard.git
cd sales-analytics-dashboard
2. Create and Activate a Virtual Environment (Recommended)python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
3. Install Python Dependenciespip install -r requirements.txt
The requirements.txt file should contain:pyspark
pandas
Flask
Flask-Cors
kagglehub
4. Run the Data PipelineThis step will download the raw sales data, process it, and create the sales_analytics.db SQLite file and Parquet outputs.spark-submit src/etl/data_pipeline.py
Expected Output:You will see logging messages indicating the progress of data ingestion, cleaning, aggregation, and storage to SQLite and Parquet. Upon successful completion, a sales_analytics.db file will be created in the root directory, and an output/parquet directory will contain Parquet files.5. Run the Flask APIThis step will start the backend server that serves data to the dashboard.python src/api/app.py
Expected Output:The Flask application will start, typically on http://127.0.0.1:5000/. You should see messages like: * Serving Flask app 'app'
 * Debug mode: on
...
 * Running on http://0.0.0.0:5000
Press CTRL+C to quit
6. View the DashboardOpen the public/index.html file in your web browser.# Example (adjust based on your OS and default browser)
open public/index.html
The dashboard will load and fetch data from the running Flask API, displaying the sales analytics.SQL ScriptsThe sql/ directory contains SQL scripts that conceptually represent the transformations and aggregations performed by the PySpark data pipeline. These are provided for reference to understand the logic behind the data models if the data were to be processed directly in a relational database.category_wise_avg_discount.sql: Calculates average discount per product line.customers_high_discount.sql: Identifies customers with an average discount usage above 10%.monthly_sales_by_region.sql: Aggregates total sales by year, month, and country.profitable_categories.sql: Summarizes total profit for each product line.sales_above_threshold.sql: Filters orders with sales exceeding a $5,000 threshold.top_10_customers_by_profit.sql: Identifies the top 10 customers based on their total profit.Note: These SQL scripts assume a conceptual sales_data_cleaned table which would be the output of the data cleaning steps.Sample OutputsSince direct CSV files or screenshots cannot be provided in a README, here's a description of the type of data you can expect from the API endpoints once the pipeline has run successfully:sales_analytics.db (SQLite Database)The SQLite database will contain tables corresponding to the generated aggregations and reporting queries:monthly_sales_by_regiontop_10_customers_by_profitcategory_wise_avg_discountsales_above_thresholdprofitable_categoriescustomers_high_discountExample API Response (GET /api/kpis){
  "totalSales": "2.8M",
  "totalProfit": "0.8M",
  "avgDiscount": "16.1%",
  "totalOrders": "2,823"
}
Example API Response (GET /api/monthly_sales?country=USA)[
  {
    "SalesYear": 2003,
    "SalesMonth": 1,
    "TotalSales": 29699.53
  },
  {
    "SalesYear": 2003,
    "SalesMonth": 2,
    "TotalSales": 27357.55
  },
  ...
]
Future EnhancementsAuthentication: Implement user authentication for the API and dashboard.Dynamic Thresholds: Allow users to define the sales threshold for "High-Value Orders" via the dashboard.More KPIs: Add more insightful KPIs such as average order value, sales growth rate, etc.Interactive Maps: Integrate a map visualization to show sales distribution by country/region.Time Period Filters: Add custom date range filters for charts.Production Deployment: Dockerize the application for easier deployment to cloud platforms.Data Source Extensibility: Modify the pipeline to ingest data from other sources (e.g., cloud storage, external APIs).Error Reporting: Implement more robust error logging and monitoring.LicenseThis project is open-source and available under the MIT License.
=======
# Sales-Intelligence-Dashboard
>>>>>>> 195f5a98b35a4673322b605d2829cf6c71ec7a13
