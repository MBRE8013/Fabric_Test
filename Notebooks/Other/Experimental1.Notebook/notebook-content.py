# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import t

# ===============================
# 1) Helpers
# ===============================
def quarter_label(date):
    return date.strftime("%b-%y")

def ols_forecast_with_pi(y: np.ndarray, periods: int, interval_width: float = 0.80):
    """
    OLS linear trend forecast with prediction intervals.
    y: historical values (n,)
    periods: number of future points
    interval_width: e.g., 0.80 for 80% prediction interval
    Returns: yhat_future, lower_future, upper_future
    """
    n = len(y)
    x = np.arange(n, dtype=float)              # 0..n-1
    x_future = np.arange(n, n + periods, dtype=float)

    # OLS closed form
    x_bar = x.mean()
    y_bar = y.mean()
    Sxx = np.sum((x - x_bar) ** 2)
    Sxy = np.sum((x - x_bar) * (y - y_bar))

    b1 = Sxy / Sxx
    b0 = y_bar - b1 * x_bar

    yhat = b0 + b1 * x
    resid = y - yhat

    # Residual standard error
    dof = max(n - 2, 1)
    s = np.sqrt(np.sum(resid ** 2) / dof)

    # t critical value
    alpha = 1.0 - interval_width
    tcrit = t.ppf(1 - alpha / 2, df=dof)

    # Prediction interval standard error at x0:
    # s * sqrt(1 + 1/n + (x0-xbar)^2 / Sxx)
    # (the leading 1 is what makes it a *prediction* interval, not just mean CI)
    se_pred = s * np.sqrt(1 + (1 / n) + ((x_future - x_bar) ** 2) / Sxx)

    yhat_future = b0 + b1 * x_future
    lower_future = yhat_future - tcrit * se_pred
    upper_future = yhat_future + tcrit * se_pred

    return yhat_future, lower_future, upper_future

def enforce_non_crossing_per_quarter(future_df: pd.DataFrame, pct_order):
    """
    Optional: ensure p25 <= p50 <= p75 <= p99 for each future quarter.
    Applies to Forecast and bounds independently using cumulative max logic.
    """
    out = future_df.copy()
    for q in out["Quarter"].unique():
        sub = out[out["Quarter"] == q].set_index("Percentile").loc[pct_order]

        # Enforce ordering via cumulative max (monotone increasing)
        for col in ["Lower Bound", "Forecast", "Upper Bound"]:
            vals = sub[col].values
            vals_fixed = np.maximum.accumulate(vals)
            sub[col] = vals_fixed

        out.loc[out["Quarter"] == q, ["Lower Bound", "Forecast", "Upper Bound"]] = sub[
            ["Lower Bound", "Forecast", "Upper Bound"]
        ].values
    return out

# ===============================
# 2) Historical Data (your example)
# ===============================
data = {
    "ds": pd.to_datetime([
        "2023-03-31", "2023-06-30", "2023-09-30",
        "2023-12-31", "2024-03-31"
    ]),
    "p25": [11.90, 14.65, 18.19, 20.27, 20.81],
    "p50": [20.92, 23.36, 27.24, 29.30, 30.27],
    "p75": [32.26, 34.06, 37.79, 39.95, 41.22],
    "p99": [68.08, 68.39, 70.53, 74.81, 76.07],
}
df = pd.DataFrame(data)
df["label"] = df["ds"].apply(quarter_label)

# ===============================
# 3) Forecast Settings
# ===============================
periods = 3                 # Jun-24, Sep-24, Dec-24
interval_width = 0.80       # 80% prediction intervals (similar spirit to your Prophet 0.8)
enforce_non_crossing = True # set False if you want "raw" independent percentile trends

percentile_cols = ["p25", "p50", "p75", "p99"]
pct_labels = ["25th", "50th", "75th", "99th"]

# Future quarter labels (based on your ds being quarter-end)
future_dates = pd.date_range(start=df["ds"].max(), periods=periods + 1, freq="Q")[1:]
future_labels = [quarter_label(d) for d in future_dates]

# ===============================
# 4) Generate Forecast Table (LONG)
# ===============================
rows = []
for col, p_label in zip(percentile_cols, pct_labels):
    y = df[col].to_numpy(dtype=float)

    yhat_f, lo_f, hi_f = ols_forecast_with_pi(y, periods=periods, interval_width=interval_width)

    for i in range(periods):
        rows.append({
            "Quarter": future_labels[i],
            "Percentile": p_label,
            "Lower Bound": round(lo_f[i], 3),
            "Forecast": round(yhat_f[i], 3),
            "Upper Bound": round(hi_f[i], 3),
        })

forecast_table = pd.DataFrame(rows)

# Order nicely: Quarter then Percentile
forecast_table["Quarter"] = pd.Categorical(forecast_table["Quarter"], categories=future_labels, ordered=True)
forecast_table["Percentile"] = pd.Categorical(forecast_table["Percentile"], categories=pct_labels, ordered=True)
forecast_table = forecast_table.sort_values(["Quarter", "Percentile"]).reset_index(drop=True)

# Optional: enforce percentile non-crossing per quarter (recommended for percentiles)
if enforce_non_crossing:
    forecast_table = enforce_non_crossing_per_quarter(forecast_table, pct_labels)
    # re-round after adjustments
    for c in ["Lower Bound", "Forecast", "Upper Bound"]:
        forecast_table[c] = forecast_table[c].round(3)

print("\nFORECAST TABLE – OLS TREND (ORDERED)\n")
print(forecast_table)


# ===============================
# 6) Plot with Data Labels (3 decimals)
# ===============================
plt.figure(figsize=(14, 7))

colors = {
    "25th": "tab:blue",
    "50th": "tab:green",
    "75th": "tab:purple",
    "99th": "tab:pink"
}

label_offset = 0.6

for col, p_label in zip(percentile_cols, pct_labels):
    color = colors[p_label]

    # Actuals
    plt.plot(df["label"], df[col], marker="o", color=color, label=f"{p_label} Actual")
    for x, yv in zip(df["label"], df[col]):
        plt.text(x, yv + label_offset, f"{yv:.3f}", ha="center", fontsize=8, color=color)

    # Forecasts + PI from forecast_table
    sub = forecast_table[forecast_table["Percentile"] == p_label]

    plt.plot(sub["Quarter"], sub["Forecast"], linestyle="--", marker="o", color=color, label=f"{p_label} Forecast")
    for x, yv in zip(sub["Quarter"], sub["Forecast"]):
        plt.text(x, yv + label_offset, f"{yv:.3f}", ha="center", fontsize=8, color=color)

    plt.fill_between(sub["Quarter"], sub["Lower Bound"], sub["Upper Bound"], alpha=0.15, color=color)

plt.title("TPS Percentile Forecasts – OLS Trend with Prediction Intervals")
plt.xlabel("Quarter")
plt.ylabel("TPS Score")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
