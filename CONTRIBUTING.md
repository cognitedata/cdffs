## Development Instructions

### Setup

Get the code!

```bash
git clone git@github.com:cognitedata/cdffs.git
cd cdffs
```

We use [poetry](https://pypi.org/project/poetry/) for dependency- and virtual environment management.

Install dependencies and initialize a shell within the virtual environment, with these commands:

```bash
poetry install --without test
poetry shell
```

Install pre-commit hooks to run static code checks on every commit:

```bash
pre-commit install
```

### Integration tests 

Choose your own CDF Project to run integration tests.

- Install test dependencies with the below command.

    ```bash
    poetry install --only test
    ```
- Create a dataset with name as `Integration Tests` and External Id as `dataset:integration_tests`
- Set the below environment variables.
    - `CLIENT_ID`
    - `CLIENT_SECRET`
    - `TENANT_ID`
    - `COGNITE_PROJECT`
    - `CDF_CLUSTER`

### Testing

Run unit tests with a following command from the root directory:

```bash
pytest tests/tests_spec.py
```

Run integration tests with a following command from the root directory:

```bash
pytest tests/tests_spec_integration.py
```

Generate code coverage reports with a following command from the root directory:

```bash
coverage run -m pytest tests/tests_spec.py
coverage report -m
```

### Code Style

Use [google style guide](https://google.github.io/styleguide/pyguide.html)

### Documentation
Build html files of documentation locally by running

```bash
cd docs
make html
```

Open `build/html/index.html` to look at the result.
