# CDM BFI

### Installing dependencies
To install dependencies through `Poetry`:
```commandline
poetry install
```

To install dependencies through `pip`:
```commandline
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
```

### Generating dependencies
To generate `requirements.txt` & `requirements-dev.txt` files:
```commandline
poetry run task requirements
```

## Package
To package this project, copy the following files into the cluster:
- src/
- hive-site.xml
- log4j.properties
- features.py
- inference.py
- train.py
- run.py
- [h2o_pysparkling_2.4-3.36.1.2-1-2.4.zip](https://s3.amazonaws.com/h2o-release/sparkling-water/spark-2.4/3.36.1.2-1-2.4/index.html)

Then create a venv:
```commandline
python -m venv ./venv
source venv/bin/activate
pip install h2o-pysparkling-2.4
```

## Run
To run the features job:
```commandline
sh run.sh feature
```

To run the training job:
```commandline
sh run.sh train
```

To run the inference job:
```commandline
sh run.sh infer
```

## Tests
To run the unit tests (with coverage):
```commandline
poetry run task test
```

## Linting
To format the code:
```commandline
poetry run task format
```

To run mypy type checks:
```commandline
poetry run task type
```
