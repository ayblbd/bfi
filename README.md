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
- log4j.properties
- main.py
- run.sh

Then create a venv:

```commandline
python -m venv ./venv
source venv/bin/activate
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
