"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import os
import glob
import zipfile
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
     # --- Rutas de entrada y salida ---
    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    # --- Cargar todos los CSV comprimidos ---
    zip_files = glob.glob(os.path.join(input_dir, "*.csv.zip"))
    dataframes = []

    for zip_path in zip_files:
        with zipfile.ZipFile(zip_path, "r") as archive:
            csv_files = [f for f in archive.namelist() if f.endswith(".csv")]
            for csv_name in csv_files:
                with archive.open(csv_name) as f:
                    dataframes.append(pd.read_csv(f))

    # Unir todos los archivos en un solo DataFrame
    df = pd.concat(dataframes, ignore_index=True)

    # 
    # 1. Construcción del archivo client.csv
    df_client = df[[
        "client_id", "age", "job", "marital", "education",
        "credit_default", "mortgage"
    ]].copy()

    # Limpieza de columnas categóricas
    df_client["job"] = (
        df_client["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    df_client["education"] = (
        df_client["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )

    # Conversión yes/no -> 1/0
    df_client["credit_default"] = df_client["credit_default"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )
    df_client["mortgage"] = df_client["mortgage"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )

    df_client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # 2. Construcción del archivo campaign.csv
    
    df_campaign = df[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    # Codificación de outcomes
    df_campaign["previous_outcome"] = df_campaign["previous_outcome"].apply(
        lambda x: 1 if str(x).lower() == "success" else 0
    )
    df_campaign["campaign_outcome"] = df_campaign["campaign_outcome"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )

    # Conversión day + month → fecha YYYY-MM-DD
    month_map = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    df_campaign["month"] = df_campaign["month"].str.lower().map(month_map)
    df_campaign["day"] = df_campaign["day"].astype(int).astype(str).str.zfill(2)

    df_campaign["last_contact_date"] = (
        "2022-" + df_campaign["month"] + "-" + df_campaign["day"]
    )

    df_campaign.drop(columns=["day", "month"], inplace=True)
    df_campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # 3. Construcción del archivo economics.csv
    df_economics = df[[
        "client_id", "cons_price_idx", "euribor_three_months"
    ]].copy()

    df_economics.to_csv(os.path.join(output_dir, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()