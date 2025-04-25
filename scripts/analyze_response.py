from pyspark.sql.functions import avg, count, col, round

# Group by Education and Response, count customers
grouped_df = df.groupBy("Education", "Response").agg(
    count("*").alias("Customer_Count"),
    round(avg("Income"), 0).alias("Avg_Income")
)

grouped_df.show()

# Convert to Pandas for plotting
grouped_pd = grouped_df.toPandas()
