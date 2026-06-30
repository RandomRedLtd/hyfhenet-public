from __future__ import annotations

import math
from typing import Any


def fit_ridge_regression(
    x: list[list[float]],
    y: list[float],
    alpha: float,
    round_digits: int | None = None,
) -> dict[str, Any]:
    means = _column_means(x)
    scales = _column_scales(x, means)
    z = [[1.0, *[(value - mean) / scale for value, mean, scale in zip(row, means, scales)]] for row in x]
    size = len(z[0])
    gram = [[0.0 for _ in range(size)] for _ in range(size)]
    rhs = [0.0 for _ in range(size)]

    for row, target in zip(z, y):
        for i in range(size):
            rhs[i] += row[i] * target
            for j in range(size):
                gram[i][j] += row[i] * row[j]

    for index in range(1, size):
        gram[index][index] += alpha

    weights = _solve_linear_system(gram, rhs)
    if round_digits is None:
        return {
            "intercept": weights[0],
            "coefficients": weights[1:],
            "feature_means": means,
            "feature_scales": scales,
        }
    return {
        "intercept": round(weights[0], round_digits),
        "coefficients": [round(value, round_digits) for value in weights[1:]],
        "feature_means": [round(value, round_digits) for value in means],
        "feature_scales": [round(value, round_digits) for value in scales],
    }


def predict_ridge_regression(row: list[float], model: dict[str, Any]) -> float:
    total = float(model["intercept"])
    for value, mean, scale, coefficient in zip(
        row,
        model["feature_means"],
        model["feature_scales"],
        model["coefficients"],
    ):
        total += ((value - mean) / scale) * coefficient
    return total


def _column_means(rows: list[list[float]]) -> list[float]:
    return [sum(row[index] for row in rows) / len(rows) for index in range(len(rows[0]))]


def _column_scales(rows: list[list[float]], means: list[float]) -> list[float]:
    scales = []
    for index, mean in enumerate(means):
        variance = sum((row[index] - mean) ** 2 for row in rows) / len(rows)
        scale = math.sqrt(max(variance, 0.0))
        scales.append(scale if scale > 1e-12 else 1.0)
    return scales


def _solve_linear_system(matrix: list[list[float]], vector: list[float]) -> list[float]:
    size = len(vector)
    augmented = [row[:] + [value] for row, value in zip(matrix, vector)]

    for pivot_index in range(size):
        best_row = max(
            range(pivot_index, size),
            key=lambda row_index: abs(augmented[row_index][pivot_index]),
        )
        if abs(augmented[best_row][pivot_index]) < 1e-12:
            augmented[best_row][pivot_index] = 1e-12
        if best_row != pivot_index:
            augmented[pivot_index], augmented[best_row] = augmented[best_row], augmented[pivot_index]

        pivot = augmented[pivot_index][pivot_index]
        for column in range(pivot_index, size + 1):
            augmented[pivot_index][column] /= pivot

        for row_index in range(size):
            if row_index == pivot_index:
                continue
            factor = augmented[row_index][pivot_index]
            if factor == 0:
                continue
            for column in range(pivot_index, size + 1):
                augmented[row_index][column] -= factor * augmented[pivot_index][column]

    return [augmented[row_index][size] for row_index in range(size)]
