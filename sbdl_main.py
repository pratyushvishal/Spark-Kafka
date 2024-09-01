import sys
from lib import Utils
from lib.logger import Log4j

if __name__ == '__main__':
    # Check if enough arguments are provided
    if len(sys.argv) < 3:
        # Hardcode default values if arguments are missing
        job_run_env = "local"  # Replace with your desired default environment
        load_date = "2024-08-31"  # Replace with your desired default date
        print("Arguments are missing; using default values: job_run_env='local', load_date='2024-08-31'")
    else:
        # Use the arguments provided by the user
        job_run_env = sys.argv[1].upper()
        load_date = sys.argv[2]

    # Initialize Spark and logger with debug statements
    try:
        print("Initializing Spark session...")
        spark = Utils.get_spark_session(job_run_env)
        print("Spark session initialized successfully.")

        # Set Spark log level to DEBUG for detailed output
        spark.sparkContext.setLogLevel("DEBUG")

        print("Creating logger...")
        logger = Log4j(spark)
        print("Logger created successfully.")
        logger.info("Finished creating Spark Session")

    except Exception as e:
        print(f"An error occurred during initialization: {e}")
        sys.exit(-1)

    # Additional logging or other operations
    try:
        # Your processing code here
        logger.info(f"Running the job for environment: {job_run_env} and load date: {load_date}")
        
        # Example Spark operations
        # df = spark.read.csv('path/to/your/csv/file.csv')
        # logger.info(f"Data read successfully with {df.count()} records.")

        # End of the script
        logger.info("Script execution completed successfully.")

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        logger.error(f"An error occurred during processing: {e}")
        sys.exit(-1)
