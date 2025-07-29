from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, hour
from pyspark.sql import SparkSession

class savedata:
    """
    A class to save Spark DataFrames into partitioned Parquet files.
    This class focuses solely on the saving functionality, separating it
    from data extraction and transformation.
    """

    def __init__(self):
        """
        Initializes the ParquetSaver. No SparkSession is needed here as it
        operates on an already existing DataFrame.
        """
        pass

    def save_partitioned_dataframe(self, df: DataFrame, output_path: str):
        """
        Adds year, month, day, and hour columns to the DataFrame based on the
        'DateTime' column and saves it as a partitioned Parquet file.

        Args:
            df (pyspark.sql.DataFrame): The input Spark DataFrame containing
                                        a 'DateTime' column.
            output_path (str): The path where the partitioned Parquet data
                               will be saved.
        """
        if df is None:
            print("Error: DataFrame is None. Cannot save partitioned Parquet.")
            return

        # Check if 'DateTime' column exists in the DataFrame
        if "DateTime" not in df.columns:
            print("Error: 'DateTime' column not found in the DataFrame. "
                  "This column is required for partitioning.")
            return

        print(f"Adding time-based columns and saving to partitioned Parquet at '{output_path}'...")

        try:
            df_with_time_cols = df.withColumn("year", year(col("DateTime"))) \
                                  .withColumn("month", month(col("DateTime"))) \
                                  .withColumn("day", dayofmonth(col("DateTime"))) \
                                  .withColumn("hour", hour(col("DateTime")))

            df_with_time_cols.write \
                .mode("overwrite") \
                .partitionBy("year", "month", "day", "hour") \
                .parquet(output_path)

            print("Data partitioned by year/month/day/hour and saved successfully.")
        except Exception as e:
            print(f"Error saving partitioned Parquet file: {e}")