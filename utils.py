import requests
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable,write_deltalake

def get_data(url_base,endpoint,params=None,headers=None):
    """

    Args:
        url_base (str): _description_
        endpoint (str): _description_
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

    Args:
        df (_type_): _description_
        path (_type_): _description_
        predicate (_type_): _description_
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

    
