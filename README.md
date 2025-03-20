# CDM BFI

## Project structue

The following is a deconstruction of each folder and what it contains in term of code:

```plaintext
+-- src
|   +-- common
|   |   +-- table_loader.py
|   |   +-- table_writer.py
|   |   +-- utils.py
|   +-- job
|   |   +-- bfi.py
|   |   +-- ods.py
|   +-- transformation
|   |   +-- bfi.py
|   |   +-- common.py
|   |   +-- ods.py
+-- config.ini
+-- log4j.properties
+-- run.sh
```

* `src`: The main directory containing all source code files and subdirectories for different functionalities.
    * `common`: The directory including common utility scripts that are used across multiple jobs in the project.
        * `table_loader.py`: Contains functions to load data from various sources with different dates and frequencies into Spark DataFrame.
        * `table_writer.py`: Contains functions to write DataFrame into table format.
        * `utils.py`: A collection of utility functions that aid in: creating `SparkSession`, loading config...
    * `job`: Contains scripts that define and launch the main Spark job.
        * `bfi.py`: Implement the main function for the BFI job, in which the follwing takes place:
            1. Load config based on the specified `env`;
            2. Create `SparkSession` based on the config loaded;
            3. Load the provided `partitiondate` or calculate it;
            4. Load the needed tables for executing the necessary transformations: Each table is loaded according to its usage in that job:
                - `load_table_from`: Load a table based on a specific date (`partitiondate`);
                - `load_latest_date_from_table`: Load the latest available `partititondate` of a specific table;
                - `load_iceberg_table_from`: Load an Iceberg table, available in `SOCLE`
                - `load_table_with_history_from`: Load a table with history of 5 days before current date + last day of the last month + last day of the last year.
            5. Execute the transformations imported from `transformation` package on the unitary tables.
            6. Write the table into the `SOCLE`/`APPLICATION`.
        * `ods.py`: Contains the job logic for ODS. It follows the same pattern as `bfi.py`.
    * `transformation`: The directory is for scripts that handle data transformations.
        * `bfi.py`: Contains transformation logic specific to BFI. This may include data cleaning, normalization, and enrichment processes. This file is devided into functions. Each function represent a logical unit of work (Mainly a table in SOCLE, but could be smaller). A function can be a grouping of smaller functions to facilitate the unit testing. But it's recommended to expose one builder function that return a DataFrame to be persisted.
        * `common.py`: General transformation functions that can be applied across different datasets and jobs.
        * `ods.py`: Transformation logic tailored for ODS data, similar to `bfi.py` but specific to ODS.
* `run.sh`: Is the script used to launch various jobs located in `job` folder using the following command:
    ```bash
    sh run.sh <job_name> <env> <partitiondate>
    ```

    * `<job_name>`: Is the name specific inside the `run.sh` to run a specific job for example: main (backward compatibility reasons) and bfi runs the `bfi.py`
    * `<env>`: dev, rct, or prd to specify the env.
    * `<partitiondate>`: Specify the partitiondate in the following format: yyyy-MM-dd

    Example of usage:

    ```bash
    sh run.sh bfi dev "2024-06-21"
    ```
* `config.ini`: Contains configuration per env.
* `log4j.properties`: Controls the level of logging through the param: `log4j.rootCategory=`, can be `INFO`, `WARN`, or `ERROR`.

## Package

To package this project, copy the following files into the cluster:

```plaintext
+-- src/
+-- config.ini
+-- log4j.properties
+-- run.sh
```

then run the `run.sh` file

## Installing dependencies

To install dependencies through `Poetry`:

```commandline
poetry install
```

To install dependencies through `pip`:

```commandline
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

## Tests

To run the unit tests (with coverage):

```commandline
poetry run poe test
```

## Linting

To format the code:

```commandline
poetry run poe format
```

To run mypy type checks:

```commandline
poetry run poe type
```

## Debug

Launch Iceberg pyspark shell:

```commandLine
pyspark --master yarn --jars /opt/spark/jars/iceberg-spark-runtime-3.3_2.12-1.4.1.jar --conf spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.iceberg_catalog.type=hadoop --conf spark.sql.catalog.iceberg_catalog.warehouse=hdfs:///iceberg/warehouse --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

## Generating dependencies

To generate `requirements.txt` & `requirements-dev.txt` files:

```commandline
poetry run task requirements
```

## Seting up pre-commit

### Install Pre-Commit

Ensure you have pre-commit installed. If you don’t, you can install it using:
```commandline
pip install pre-commit
```

### Install the Hooks

install the defined hooks using:
```commandline
pre-commit install
```

### Run the Hooks Manually

If you want to manually run the hooks before committing, you can use:
```commandline
pre-commit run
```

### Committing Changes

Now, every time you commit, the hooks will automatically run and check your code.
