import logging
import argparse, sys
import zipfile
from pathlib import Path
import pandas
from concrete.ml.deployment import FHEModelDev
from concrete.ml.sklearn import RandomForestRegressor, DecisionTreeRegressor
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
import pickle
import time

LOG = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True, choices=["nilm", "cohort", "forecast"], help="Model to train: nilm | cohort | forecast")
    parser.add_argument("--overwrite", help="Overwrite existing model files if they exist")
    args = parser.parse_args()

    PARENT_DIR = Path(__file__).parent.resolve()
    MODEL_PATH = PARENT_DIR / "models" / args.model
    DATASET_PATH = PARENT_DIR / "datasets" / f"{args.model}.csv"

    if not MODEL_PATH.is_dir():
        MODEL_PATH.mkdir(parents=True)
    else:
        if args.overwrite:
            for file in MODEL_PATH.rglob("*"):
                file.unlink()
            MODEL_PATH.rmdir()
        else:
            LOG.error(f"Model directory at {MODEL_PATH} already exists, if you want to overwrite the directory set --overwrite=true")
            sys.exit()

    LOG.info(f"Starting the training process for {args.model}")

    target_columns = []
    metadata_columns = []
    one_hot_columns = []
    standard_scaler_columns = []
    model = None

    match args.model:
        case "nilm":
            model = RandomForestRegressor(max_depth=6, n_estimators=8)
            target_columns = [
                "target_plug_1_power_w",
                "target_plug_2_power_w",
                "target_hvac_power_w",
                "target_baseload_power_w",
                "target_other_power_w"
            ]
            one_hot_columns = [
                "day_of_week",
                "load_event_type_code",
                "load_event_direction_code"
            ]
            standard_scaler_columns = [
                "minute_of_day",
                "minute_sin",
                "minute_cos",
                "dow_sin",
                "dow_cos",
                "linky_household_power_w",
                "linky_household_power_mean_1h_w",
                "linky_household_power_std_1h_w",
                "linky_household_power_delta_1h_w",
                "linky_household_power_mean_24h_w",
                "linky_household_power_std_24h_w",
                "plug_1_power_w",
                "plug_1_power_mean_1h_w",
                "plug_2_power_w",
                "plug_2_power_mean_1h_w",
                "monitored_plug_share",
                "indoor_temperature_c",
                "indoor_humidity_pct",
                "temperature_delta_1h_c"
            ]
            pass
        case "cohort":
            model = DecisionTreeRegressor()
            target_columns = ["cohort_label_code"]
            # cohort_label is label metadata; using it as input leaks the target.
            metadata_columns = ["cohort_label"]
            one_hot_columns = []
            standard_scaler_columns = [
                "day_index",
                "total_energy_kwh",
                "mean_power_w",
                "peak_power_w",
                "p95_power_w",
                "min_power_w",
                "load_factor",
                "peak_to_mean_ratio",
                "morning_energy_share",
                "evening_energy_share",
                "overnight_energy_share",
                "temperature_mean_c",
                "temperature_sensitivity_w_per_c",
                "monitored_plug_energy_share",
                "hvac_energy_share",
                "baseload_mean_w",
                "event_rate_per_day",
                "flexibility_score",
            ]
        case "forecast":
            model = RandomForestRegressor(max_depth=6, n_estimators=8)
            target_columns = ["target_household_power_w"]
            one_hot_columns = [
                "day_of_week",
                "load_event_type_code",
                "load_event_direction_code"
            ]
            standard_scaler_columns = [
                "horizon_minutes",
                "minute_of_day",
                "minute_sin",
                "minute_cos",
                "dow_sin",
                "dow_cos",
                "target_minute_of_day",
                "target_day_of_week",
                "target_minute_sin",
                "target_minute_cos",
                "target_dow_sin",
                "target_dow_cos",
                "linky_household_power_w",
                "linky_household_power_mean_1h_w",
                "linky_household_power_std_1h_w",
                "linky_household_power_delta_1h_w",
                "linky_household_power_mean_24h_w",
                "linky_household_power_std_24h_w",
                "plug_1_power_w",
                "plug_1_power_mean_1h_w",
                "plug_2_power_w",
                "plug_2_power_mean_1h_w",
                "monitored_plug_share",
                "indoor_temperature_c",
                "indoor_humidity_pct",
                "temperature_delta_1h_c"
            ]
        case _:
            LOG.error(f"Model {args.model} not supported")
            sys.exit()

    LOG.info("Preparing the training data...")

    data_X = pandas.read_csv(DATASET_PATH)
    data_y = data_X[target_columns].copy()
    data_X = data_X.drop(columns=[*target_columns, *metadata_columns])

    X_train, X_test, y_train, y_test = train_test_split(data_X, data_y, train_size=0.8, random_state=1)

    data_pre_processor = ColumnTransformer(
        transformers=[
            ("one_hot", OneHotEncoder(sparse_output=False), one_hot_columns),
            ("standard_scaler", StandardScaler(), standard_scaler_columns)
        ],
        remainder="passthrough",
        verbose_feature_names_out=False
    )

    pre_processed_X_train = data_pre_processor.fit_transform(X_train)
    pre_processed_X_test = data_pre_processor.transform(X_test)

    LOG.info("Training data prepared!")

    LOG.info(f"Starting training for {args.model} model...")

    start = time.time()

    model.fit(pre_processed_X_train, y_train)

    score = model.score(pre_processed_X_test, y_test)

    LOG.info(f"Training done for {args.model} model, time elapsed: {((time.time() - start) * 1000):.2f} milliseconds, score: {score}")

    LOG.info("Compiling the model...")

    start = time.time()

    model.compile(pre_processed_X_train)

    LOG.info(f"Compilation done for {args.model} model, time elapsed: {((time.time() - start) * 1000):.2f} milliseconds")

    fhe_model = FHEModelDev(MODEL_PATH, model)

    fhe_model.save(via_mlir=True)

    LOG.info(f"Saving the model files under {MODEL_PATH}...")

    pickle.dump(model.sklearn_model, (MODEL_PATH / "model.pkl").open("wb"))

    pickle.dump(data_pre_processor, (MODEL_PATH / "data-pre-processor.pkl").open("wb"))

    client_files = zipfile.ZipFile(MODEL_PATH / "client-files.zip", mode="w")

    client_files.write(MODEL_PATH / "client.zip", "client.zip", compress_type=zipfile.ZIP_DEFLATED)
    client_files.write(MODEL_PATH / "data-pre-processor.pkl", "data-pre-processor.pkl", compress_type=zipfile.ZIP_DEFLATED)

    client_files.close()

    LOG.info("Model files saved!")

    LOG.info("Done!")

if __name__ == "__main__":
    main()
