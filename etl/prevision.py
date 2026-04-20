import numpy as np
import pandas as pd
from pathlib import Path
from sqlalchemy import text


def _first_day_of_month(value):
    ts = pd.Timestamp(value)
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)


def ensure_previsions_table(engine):
    create_sql = """
    IF OBJECT_ID('dbo.Previsions', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Previsions (
            PrevisionID INT PRIMARY KEY IDENTITY,
            DepartementID INT,
            Mois DATE,
            Type NVARCHAR(20),
            ChargeType NVARCHAR(20),
            ChargeValue DECIMAL(10,2),
            Modele NVARCHAR(50),
            DateCreation DATETIME DEFAULT GETDATE(),
            FOREIGN KEY (DepartementID) REFERENCES dbo.Dim_Departement(DepartementID)
        );

        CREATE INDEX IX_Previsions_Dept_Mois
            ON dbo.Previsions(DepartementID, Mois, ChargeType);
    END
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def _read_monthly_history(engine):
    sql_query = """
    SELECT
        ft.DepartementID,
        CAST(DATEFROMPARTS(dt.Annee, dt.Mois, 1) AS DATE) AS Mois,
        'Telephonique' AS ChargeType,
        CAST(SUM(ft.ForfaitTND) AS DECIMAL(10,2)) AS ChargeValue
    FROM Fact_Telephone ft
    INNER JOIN Dim_Temps dt ON dt.DateID = ft.DateID
    GROUP BY ft.DepartementID, dt.Annee, dt.Mois

    UNION ALL

    SELECT
        fi.DepartementID,
        CAST(DATEFROMPARTS(dt.Annee, dt.Mois, 1) AS DATE) AS Mois,
        'Impression' AS ChargeType,
        CAST(SUM(fi.NbPages * fi.CoutUnitaire) AS DECIMAL(10,2)) AS ChargeValue
    FROM Fact_Impression fi
    INNER JOIN Dim_Temps dt ON dt.DateID = fi.DateID
    GROUP BY fi.DepartementID, dt.Annee, dt.Mois
    """

    return pd.read_sql(sql_query, engine)


def _normalize_dept_name(value):
    return str(value).strip().upper()


def _run_notebook(notebook_path, output_path):
    import papermill as pm

    pm.execute_notebook(
        input_path=str(notebook_path),
        output_path=str(output_path),
        cwd=str(notebook_path.parent),
        log_output=True,
    )


def _read_notebook_forecasts(notebook_dir):
    notebook_dir = Path(notebook_dir)
    sources = [
        (notebook_dir / "previsions_telephoniques.csv", "Telephonique"),
        (notebook_dir / "previsions_impression.csv", "Impression"),
    ]

    frames = []
    for csv_path, charge_type in sources:
        if not csv_path.exists():
            raise FileNotFoundError(f"Fichier introuvable: {csv_path}")

        df = pd.read_csv(csv_path)
        required = {"Departement", "Mois", "Type", "Charge_TND", "Modele"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans {csv_path.name}: {sorted(missing)}")

        df = df.rename(columns={"Charge_TND": "ChargeValue"})
        df["ChargeType"] = charge_type
        df["Mois"] = pd.to_datetime(df["Mois"], errors="coerce").dt.to_period("M").dt.to_timestamp()
        df = df.dropna(subset=["Mois", "Departement", "Type", "ChargeValue"])
        df["Type"] = df["Type"].astype(str).str.strip().str.title()
        frames.append(df[["Departement", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"]])

    if not frames:
        return pd.DataFrame(columns=["Departement", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"])

    return pd.concat(frames, ignore_index=True)


def _attach_departement_id(engine, forecasts_df):
    if forecasts_df.empty:
        return pd.DataFrame(columns=["DepartementID", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"])

    dim_df = pd.read_sql("SELECT DepartementID, NomDepartement FROM dbo.Dim_Departement", engine)
    dim_df["_key"] = dim_df["NomDepartement"].map(_normalize_dept_name)

    data = forecasts_df.copy()
    data["_key"] = data["Departement"].map(_normalize_dept_name)
    merged = data.merge(dim_df[["DepartementID", "_key"]], on="_key", how="left")

    missing = sorted(merged.loc[merged["DepartementID"].isna(), "Departement"].dropna().unique().tolist())
    if missing:
        raise ValueError(
            "Departements introuvables dans Dim_Departement: " + ", ".join(missing[:10])
        )

    merged["ChargeValue"] = pd.to_numeric(merged["ChargeValue"], errors="coerce").fillna(0.0)
    return merged[["DepartementID", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"]]


def _run_notebook_ml_pipeline(dw_engine, notebook_path):
    notebook_path = Path(notebook_path)
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook introuvable: {notebook_path}")

    output_nb = notebook_path.with_name("Prevision.executed.ipynb")
    _run_notebook(notebook_path, output_nb)

    notebook_df = _read_notebook_forecasts(notebook_path.parent)
    mapped_df = _attach_departement_id(dw_engine, notebook_df)
    inserted = load_previsions(dw_engine, mapped_df)

    return {
        "rows_history": int((mapped_df["Type"] == "Historique").sum()) if not mapped_df.empty else 0,
        "rows_forecast": int((mapped_df["Type"] == "Prevision").sum()) if not mapped_df.empty else 0,
        "rows_inserted": int(inserted),
        "source": "notebook_ml",
    }


def _forecast_series(history_df, horizon_months=12):
    if history_df.empty:
        return pd.DataFrame(columns=["DepartementID", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"])

    history_df = history_df.copy()
    history_df["Mois"] = pd.to_datetime(history_df["Mois"]).apply(_first_day_of_month)
    history_df = history_df.sort_values(["DepartementID", "ChargeType", "Mois"]).reset_index(drop=True)

    historique = history_df.copy()
    historique["Type"] = "Historique"
    historique["Modele"] = "Historique"

    forecasts = []
    grouped = history_df.groupby(["DepartementID", "ChargeType"], sort=False)
    for (departement_id, charge_type), grp in grouped:
        grp = grp.sort_values("Mois")
        y = grp["ChargeValue"].astype(float).values
        n = len(y)
        if n == 0:
            continue

        if n >= 2:
            x = np.arange(n, dtype=float)
            slope, intercept = np.polyfit(x, y, 1)
        else:
            slope, intercept = 0.0, float(y[0])

        last_month = _first_day_of_month(grp["Mois"].max())
        for step in range(1, horizon_months + 1):
            month = last_month + pd.DateOffset(months=step)
            raw_pred = intercept + slope * (n - 1 + step)
            value = max(0.0, float(raw_pred))
            forecasts.append(
                {
                    "DepartementID": int(departement_id),
                    "Mois": _first_day_of_month(month),
                    "Type": "Prevision",
                    "ChargeType": charge_type,
                    "ChargeValue": round(value, 2),
                    "Modele": "LinearTrend",
                }
            )

    prevision_df = pd.DataFrame(forecasts)
    historique = historique[["DepartementID", "Mois", "Type", "ChargeType", "ChargeValue", "Modele"]]
    if prevision_df.empty:
        return historique
    return pd.concat([historique, prevision_df], ignore_index=True)


def load_previsions(engine, previsions_df):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM dbo.Previsions"))
        if previsions_df.empty:
            return 0

        rows = []
        for _, row in previsions_df.iterrows():
            rows.append(
                {
                    "DepartementID": int(row["DepartementID"]),
                    "Mois": pd.Timestamp(row["Mois"]).date(),
                    "Type": str(row["Type"]),
                    "ChargeType": str(row["ChargeType"]),
                    "ChargeValue": float(row["ChargeValue"]),
                    "Modele": str(row["Modele"]),
                }
            )

        conn.execute(
            text(
                """
                INSERT INTO dbo.Previsions (DepartementID, Mois, Type, ChargeType, ChargeValue, Modele)
                VALUES (:DepartementID, :Mois, :Type, :ChargeType, :ChargeValue, :Modele)
                """
            ),
            rows,
        )
        return len(rows)


def run_previsions_pipeline(
    dw_engine,
    horizon_months=12,
    use_notebook_ml=False,
    notebook_path=None,
):
    ensure_previsions_table(dw_engine)

    if use_notebook_ml:
        if not notebook_path:
            raise ValueError("notebook_path est requis quand use_notebook_ml=True")
        return _run_notebook_ml_pipeline(dw_engine, notebook_path)

    history = _read_monthly_history(dw_engine)
    previsions_df = _forecast_series(history, horizon_months=horizon_months)
    inserted = load_previsions(dw_engine, previsions_df)
    return {
        "rows_history": int((previsions_df["Type"] == "Historique").sum()) if not previsions_df.empty else 0,
        "rows_forecast": int((previsions_df["Type"] == "Prevision").sum()) if not previsions_df.empty else 0,
        "rows_inserted": int(inserted),
        "source": "linear_trend",
    }
