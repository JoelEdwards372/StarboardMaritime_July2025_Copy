import json
import sys
from decimal import Decimal, ROUND_DOWN

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_unixtime, round as spark_round

class etlprocess:
    """
    A class to process large JSON files in chunks, apply a typed schema,
    and convert data types using Spark.

    This class is designed to be used as a module, particularly useful in
    environments like Jupyter notebooks, for efficient processing of
    potentially large JSON datasets that might not fit into memory directly.
    """

    def __init__(self, app_name="TypedJSONProcessor", spark_config=None):
        """
        Initializes the JSONDataProcessor with a SparkSession.

        Args:
            app_name (str): The name of the Spark application.
            spark_config (dict, optional): A dictionary of Spark configuration
                                          properties. Defaults to common settings.
        """
        self.app_name = app_name
        self.spark_config = spark_config if spark_config is not None else {
            "spark.driver.memory": "4g",
            "spark.driver.maxResultSize": "2g",
            "spark.sql.adaptive.enabled": "true"
        }
        self.spark = self._initialize_spark_session()
        self.schema = self._define_schema()

    def _initialize_spark_session(self):
        """
        Initializes and returns a SparkSession based on the configured properties.
        """
        builder = SparkSession.builder.appName(self.app_name)
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)
        return builder.getOrCreate()

    def _define_schema(self):
        """
        Defines the Spark SQL schema for the incoming JSON data.
        This schema specifies the expected column names and their data types.
        """
        return StructType([
            StructField("UTCTimeStamp", IntegerType(), True),
            StructField("Message_MessageID", IntegerType(), True),
            StructField("Message_UserID", IntegerType(), True),
            StructField("Message_Latitude", DoubleType(), True),
            StructField("Message_Longitude", DoubleType(), True),
            StructField("Message_SOG", DoubleType(), True)
            # Add more numeric fields as needed based on your JSON structure
        ])

    def _truncate_decimal(self, value, decimal_places=6):
        """
        Truncates a decimal value to a specified number of decimal places.
        Uses Python's Decimal type for precise handling before converting back to float.

        Args:
            value (float or str or None): The numeric value to truncate.
            decimal_places (int): The number of decimal places to truncate to.

        Returns:
            float or None: The truncated float value, or None if input is invalid.
        """
        if value is None:
            return None
        try:
            # Convert to Decimal for precise truncation
            decimal_val = Decimal(str(value))
            multiplier = Decimal(10 ** decimal_places)
            return float(decimal_val.quantize(Decimal('1') / multiplier, rounding=ROUND_DOWN))
        except (ValueError, TypeError):
            # Handle cases where value cannot be converted to Decimal
            return None

    def _safe_convert_int(self, value):
        """
        Safely converts a value to an integer. Handles None, empty strings,
        and strings that represent floats (e.g., "123.0").

        Args:
            value (str or int or float or None): The value to convert.

        Returns:
            int or None: The integer value, or None if conversion fails.
        """
        if value is None or value == '':
            return None
        try:
            return int(float(value))  # Handle strings like "123.0"
        except (ValueError, TypeError):
            return None

    def _safe_convert_float(self, value):
        """
        Safely converts a value to a float. Handles None and empty strings.

        Args:
            value (str or int or float or None): The value to convert.

        Returns:
            float or None: The float value, or None if conversion fails.
        """
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def process_file(self, file_path, max_memory_mb=2048):
        """
        Processes a JSON file in chunks, applies the defined schema, and performs
        data type conversions.

        Args:
            file_path (str): The path to the input JSON file.
            max_memory_mb (int): The maximum memory (in MB) to use for each chunk
                                 before converting it to a Spark DataFrame.

        Returns:
            pyspark.sql.DataFrame or None: A Spark DataFrame containing the
                                           processed data, or None if no data
                                           was processed.
        """
        max_memory_bytes = max_memory_mb * 1024 * 1024
        chunk_records = []
        current_memory = 0
        all_dataframes = []
        chunk_number = 0
        total_processed = 0

        print(f"Processing '{file_path}' with typed schema in chunks of ~{max_memory_mb}MB...")

        try:
            with open(file_path, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        message = record.get('Message', {})

                        # Create properly typed record dictionary
                        flat_record = {
                            'UTCTimeStamp': self._safe_convert_int(record.get('UTCTimeStamp')),
                            'Message_MessageID': self._safe_convert_int(message.get('MessageID')),
                            'Message_UserID': self._safe_convert_int(message.get('UserID')),
                            'Message_Latitude': self._truncate_decimal(self._safe_convert_float(message.get('Latitude')), 6),
                            'Message_Longitude': self._truncate_decimal(self._safe_convert_float(message.get('Longitude')), 6),
                            'Message_SOG': self._safe_convert_float(message.get('SOG'))
                        }

                        chunk_records.append(flat_record)
                        # Estimate memory usage (approximation)
                        current_memory += sys.getsizeof(str(flat_record))

                        if current_memory >= max_memory_bytes:
                            chunk_number += 1
                            print(f"Processing chunk {chunk_number} with {len(chunk_records):,} records...")

                            # Create DataFrame with typed schema
                            chunk_df = self.spark.createDataFrame(chunk_records, self.schema)
                            all_dataframes.append(chunk_df)

                            total_processed += len(chunk_records)
                            print(f"  Chunk {chunk_number} processed. Total so far: {total_processed:,}")

                            chunk_records = []
                            current_memory = 0

                        if line_num % 100000 == 0:
                            print(f"  Read {line_num:,} lines...")

                    except json.JSONDecodeError as e:
                        if line_num <= 10:  # Show first 10 JSON parsing errors only
                            print(f"JSON parsing error at line {line_num}: {e} - Line content: '{line.strip()[:100]}...'")
                    except Exception as e:
                        if line_num <= 10:  # Show first 10 general errors only
                            print(f"General error at line {line_num}: {e} - Line content: '{line.strip()[:100]}...'")

            # Process final chunk if any records remain
            if chunk_records:
                chunk_number += 1
                print(f"Processing final chunk {chunk_number} with {len(chunk_records):,} records...")
                chunk_df = self.spark.createDataFrame(chunk_records, self.schema)
                all_dataframes.append(chunk_df)
                total_processed += len(chunk_records)

            print(f"Total processed: {total_processed:,} records in {chunk_number} chunks")

            # Union all chunks
            if all_dataframes:
                print("Combining chunks and applying final transformations...")
                final_df = all_dataframes[0]
                for df in all_dataframes[1:]:
                    final_df = final_df.union(df)

                # Convert UTCTimeStamp to datetime and ensure lat/lon precision
                final_df = final_df.withColumn(
                    "DateTime",
                    from_unixtime(col("UTCTimeStamp")).cast(TimestampType())
                ).withColumn(
                    "Message_Latitude",
                    spark_round(col("Message_Latitude"), 6)
                ).withColumn(
                    "Message_Longitude",
                    spark_round(col("Message_Longitude"), 6)
                )

                # Reorder columns to put DateTime first
                column_order = ["DateTime", "UTCTimeStamp"] + [c for c in final_df.columns if c not in ["DateTime", "UTCTimeStamp"]]
                final_df = final_df.select(*column_order)

                print(f"Final DataFrame created with {final_df.count():,} records")
                return final_df

        except FileNotFoundError:
            print(f"Error: File not found at '{file_path}'")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during file processing: {e}")
            return None

        return None