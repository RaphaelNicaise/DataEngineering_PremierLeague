import requests
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable,write_deltalake
import json
import shutil

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

# Funciones para obtener y actualizar la metadata

def update_last_update_in_json(path,new_value):
    """
        Actualizar el valor del json, con la fecha del ultimo partido cargado
    Args:
        path (str): path del archivo json
        new_value (str): Nueva fecha del ultimo partido cargado
    """
    with(open(path,'w')) as file:
        json.dump(new_value,file)

def get_metadata_from_json(path):
    """
        Obtiene la metadata del json
    Args:
        path (_type_): path del archivo json

    Returns:
        str: la fecha del ultimo partido cargado
    """
    
    with open(path) as f:
        metadata = json.load(f)

    return metadata

def option_to_remove_delta_table(path):
    """
        Elimina la tabla delta
    Args:
        path (str): path de la tabla delta
    """
    print("La carpeta ya existe")
    while True:
        dcs = input("Quieres eliminarla? Y/N")
        if dcs.lower() == "y" or dcs.lower() == "n":
            break
        else:
            print("Opcion invalida")
            
    if dcs.lower() == "y":
        shutil.rmtree(path)
        # Al borrar la carpeta, se reinicia la fecha de actualizacion, ya que si no hay inconsistencias
        print("Carpeta eliminada correctamente")
        
    else:
        print("No se elimino la carpeta")
        
def update_positions(df_positions,team, goals_for, goals_against, points ):
    """Logica para tabla de posiciones de la liga

    Args:
        team (_type_): _description_
        goals_for (_type_): _description_
        goals_against (_type_): _description_
        points (_type_): _description_
    """
    
    if team in df_positions["team"].values:
        df_positions.loc[df_positions["team"] == team, "points"] += points
        df_positions.loc[df_positions["team"] == team, "goals_for"] += goals_for
        df_positions.loc[df_positions["team"] == team, "goals_against"] += goals_against
    else:
        df_positions.loc[len(df_positions)] = [team, points, goals_for, goals_against, goals_for - goals_against]
        
    df_positions.loc[df_positions["team"] == team, "goal_difference"] = (
        df_positions.loc[df_positions["team"] == team, "goals_for"] - 
        df_positions.loc[df_positions["team"] == team, "goals_against"]
    )
