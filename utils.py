import requests
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable,write_deltalake

def get_data(url_base,endpoint,params=None,headers=None):
    """
    Pasas la url base y el endpoint para hacer la petición y obtener los datos.
    Los params y los headers, son opcionales
    Args:
        url_base (str): la url de la api
        endpoint (str): la peticion concreta
        params (dict: Parametros de la peticion. Defaults None.
        headers (dict): Encabezados de la petición. Defaults None.
    """
    try:
        url = f"{url_base}/{endpoint}"
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        if not data:
            data = None
        
    except requests.exceptions.RequestException as e:
        data = None    
    
    return data


def merge_data(df,path,predicate):
    """
    Merge de un dataframe con un delta lake
    Args:
        df (Dataframe): dataframe con los datos actualizados 
        path (str): ruta del delta lake que se quiere actualizar
        predicate (str): condicion del join
    """
    df_pa = pa.Table.from_pandas(df)
    actual_data = DeltaTable(path)
    (
        actual_data.merge(
            source=df_pa,
            source_alias="src",
            target_alias="tgt",
            predicate=predicate
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    
