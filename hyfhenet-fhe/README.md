# HyFHE-net fhe

## 0. Structure

Datasets are located under `datasets`.

Trained models and their corresponding files (server, client, and preprocessor) are saved in `models` directory.

Currently supported models:
- NILM (RandomForestRegressor)
- Cohort (DecisionTreeRegressor)
- Forecast (RandomForestRegressor)

There are available pretrained models for amd64 Linux in the [models-pretrained-linux-amd64](./models-pretrained-linux-amd64) directory.

## 1. Running

### 1.1. Prerequisites:
- Python 3.12.12

### 1.2. Create a virtual env

`$ python -m venv .venv`

### 1.2. Install dependencies

`$ chmod +x ./install-deps.sh`

`$ ./install-deps.sh`

### 1.3. Run train script
This repository comes with a single script `main.py` through which the training is done for NILM, Forecast, and Cohort models.

To train a model run `main.py` script with the corresponding flag:

`$ python ./main.py --model=(nilm | cohort | forecast)`

Once a model is trained and its files are in `models` directory, retraining that same model with an already existing trained model will cause an error, i.e. if you already have a trained Cohort model and you try to retrain with an already existing trained Cohort model will throw an error.

Either delete the existing model directory or run the script with `--overwrite=true` flag.

Example with overwrite flag:

`$ python ./main.py --model=forecast --overwrite=true`

This will delete that model's files and train a new model.

**NOTE:** If you haven't changed that models dataset and you retrain the model, it will result in the same model.
