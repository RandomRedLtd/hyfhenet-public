#!/bin/sh
set -eu

python -m pip install --upgrade pip wheel setuptools
python -m pip install --no-cache-dir torch==2.3.1 --index-url https://download.pytorch.org/whl/cpu
python -m pip install --no-cache-dir --no-deps -r requirements-fhe.txt

python - <<'PY'
from pathlib import Path
import site

roots = []
for value in site.getsitepackages():
    roots.append(Path(value) / "concrete" / "ml" / "sklearn")
user_site = site.getusersitepackages()
if user_site:
    roots.append(Path(user_site) / "concrete" / "ml" / "sklearn")

for root in roots:
    if not root.exists():
        continue
    xgb = root / "xgb.py"
    if xgb.exists():
        xgb.unlink()
    replacements = {
        "__init__.py": [
            ("from .xgb import XGBClassifier, XGBRegressor\n", ""),
        ],
        "base.py": [
            ("from xgboost.sklearn import XGBModel\n", ""),
            ('"xgboost" if isinstance(sklearn_model, XGBModel) else ', ""),
        ],
    }
    for filename, changes in replacements.items():
        path = root / filename
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for old, new in changes:
            text = text.replace(old, new)
        path.write_text(text, encoding="utf-8")
PY
