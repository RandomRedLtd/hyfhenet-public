#!/bin/sh

python -m pip install -U pip wheel setuptools
python -m pip install torch==2.3.1 --index-url https://download.pytorch.org/whl/cpu
python -m pip install -r requirements.txt --no-deps

rm -rf .venv/lib/python3*/site-packages/concrete/ml/sklearn/xgb.py

sed -i -e '/from .xgb import XGBClassifier, XGBRegressor/d' .venv/lib/python3*/site-packages/concrete/ml/sklearn/__init__.py
sed -i -e '/from xgboost.sklearn import XGBModel/d' .venv/lib/python3*/site-packages/concrete/ml/sklearn/base.py
sed -i -e 's/"xgboost" if isinstance(sklearn_model, XGBModel) else //g' .venv/lib/python3*/site-packages/concrete/ml/sklearn/base.py
