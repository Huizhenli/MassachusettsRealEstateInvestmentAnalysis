import pathlib
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "PythonScript"))
from data_clean import clean_frame  # noqa: E402


def test_filters_state_and_interpolates_interior_gaps_only():
    df = pd.DataFrame({
        "RegionName": [" Boston ", "Seattle"],
        "State": ["MA", "WA"],
        "2020-01-31": [np.nan, 1.0],
        "2020-02-29": [100.0, 2.0],
        "2020-03-31": [np.nan, 3.0],
        "2020-04-30": [140.0, 4.0],
        "2020-05-31": [np.nan, 5.0],
    })
    out = clean_frame(df, "State")
    assert out["State"].tolist() == ["MA"]
    assert out.loc[0, "RegionName"] == "Boston"          # trimmed
    assert np.isnan(out.loc[0, "2020-01-31"])             # leading gap not invented
    assert out.loc[0, "2020-03-31"] == 120.0              # interior gap filled
    assert np.isnan(out.loc[0, "2020-05-31"])             # trailing gap not invented
