# README 
Repository containing pip installable python module to read from and write to dremio database.

## ENVIRONMENT
This library depends on some environment variables to ensure the user is authenticated.
These include:
* `GCP_PROJECT_ID`
* `GCP_BUCKET_NAME`
* `DREMIO_HOST`
* `DREMIO_USERNAME`
* `DREMIO_PASSWORD`

## SETUP
This repository is pip installable, either from source code or from the repo:

```
pip install -e ./
```

OR 

```
pip install git+https://git@github.com/waer-io/pythonlib-waerlib.git
```

## EXAMPLES
See the `example` folder for example usage of each of the waerlib functions.

```
python ./examples/read.py
python ./examples/store_raw.py
python ./examples/write.py
```
