import dlt
import duckdb

# Set pipeline name, destination, and dataset name

pipeline = dlt.pipeline(

    pipeline_name="quick_start",

    destination="duckdb",

    dataset_name="mydata",

)