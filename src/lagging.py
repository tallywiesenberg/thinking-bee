import numpy as np


def add_exp_lag_features(df, feature_cols, taus=(2, 12), L=24, time_col="timestamp"):
    """
    Add exponentially weighted lagged features to a dataframe.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    feature_cols : list
        Column names to create lagged features for.
    taus : tuple
        Time constants for exponential weights. Default is (2, 12).
    L : int
        Lookback window size. Default is 24.
    time_col : str
        Name of the timestamp column. Default is "timestamp".
    
    Returns
    -------
    pd.DataFrame
        Dataframe with new exponentially weighted lag features appended.
    """
    df = df.sort_values(time_col).copy()
    for tau in taus:
        weights = np.exp(-np.arange(1, L + 1) / tau)
        weights = weights / weights.sum()
        for col in feature_cols:
            vals = df[col].astype(float).to_numpy()
            out = np.full(len(vals), np.nan)
            for i in range(L, len(vals)):
                past = vals[i - L : i][::-1]  # x_{t-1}, x_{t-2}, ...
                out[i] = np.dot(weights, past)
            df[f"{col}_lag_tau{tau}"] = out
    return df


def add_time_lag_features(df, feature_cols=None, L=24, time_col="timestamp"):
    """
    Add simple lagged values for datetime or timedelta features.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    feature_cols : list or None
        Column names to create lagged features for. If None, all time-based
        columns are used.
    L : int
        Number of lag steps to create. Default is 24.
    time_col : str
        Name of the timestamp column. Default is "timestamp".

    Returns
    -------
    pd.DataFrame
        Dataframe with new lagged time-based feature columns appended.
    """
    df = df.sort_values(time_col).copy()
    if feature_cols is None:
        feature_cols = [
            col
            for col in df.columns
            if np.issubdtype(df[col].dtype, np.datetime64)
            or np.issubdtype(df[col].dtype, np.timedelta64)
        ]
    else:
        feature_cols = [
            col
            for col in feature_cols
            if np.issubdtype(df[col].dtype, np.datetime64)
            or np.issubdtype(df[col].dtype, np.timedelta64)
        ]

    for col in feature_cols:
        for lag in range(1, L + 1):
            df[f"{col}_lag_{lag}"] = df[col].shift(lag)
    return df


def add_rolling_sum(df, columns, window=6):
    """
    Calculate rolling sum for specified columns in a pandas dataframe.
    
    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    columns : list
        List of columns to compute rolling sum for.
    window : int
        Window size for rolling average. Default is 7.
    
    Returns
    -------
    pd.DataFrame
        Dataframe with new rolling average columns appended.
    """
    df = df.copy()
    for col in columns:
        df[f"{col}_rolling_sum_{window}"] = df[col].shift(1).rolling(window=window).sum()
    return df


