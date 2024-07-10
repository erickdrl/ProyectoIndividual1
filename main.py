from fastapi import FastAPI, HTTPException
import pandas as pd
from datetime import datetime
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

df=pd.read_csv('Movies.csv')
df2=pd.read_csv('Crew.csv')
df3=pd.read_csv('Cast.csv')

app=FastAPI()

months={
    'enero': 1,
    'febrero': 2,
    'marzo': 3,
    'abril': 4,
    'mayo': 5,
    'junio': 6,
    'julio': 7,
    'agosto': 8,
    'septiembre': 9,
    'octubre': 10,
    'noviembre': 11,
    'diciembre': 12
}

df['release_date'] = pd.to_datetime(df['release_date'])

def cantidad_filmaciones_mes(df, m):
    month_number=months.get(m.lower())
    if month_number is None:
        return f"El mes '{m}' no es válido."
    
    movies_per_month=df[df['release_date'].dt.month==month_number]
    
    for index, row in movies_per_month.iterrows():
        print(f"Nombre: {row['title']}, Fecha de estreno: {row['release_date'].date()}")
    
    q=len(movies_per_month)
    
    return f"{q} Cantidad de peliculas fueron estrenadas el mes de: {m}"

@app.get("/cantidad_filmaciones_mes/{month}")
def get_cantidad_filmaciones_mes(month: str):
    try:
        r=cantidad_filmaciones_mes(df, month)
        return {"message":r}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))



days_week={
    'lunes': 0,
    'martes': 1,
    'miércoles': 2,
    'jueves': 3,
    'viernes': 4,
    'sábado': 5,
    'domingo': 6
}

def cantidad_filmaciones_dia(df, d):

    day_number=days_week.get(d.lower())
    if day_number is None:
        return f"El día '{d}' no es válido."
    
    movies_per_day=df[df['release_date'].dt.dayofweek==day_number]
    
    for index, row in movies_per_day.iterrows():
        print(f"Nombre: {row['title']}, Fecha de estreno: {row['release_date'].date()}")
    
    q=len(movies_per_day)
    
    return f"{q} Cantidad de peliculas fueron estrenadas el día: {d}"

@app.get("/cantidad_filmaciones_dia/{day}")
def get_cantidad_filmaciones_dia(day: str):
    try:
        result=cantidad_filmaciones_dia(df, day)
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    


def score_titulo(df, t):

    movie=df[df['title']==t]
    
    if movie.empty:
        return f"La película '{t}' no se encuentra en el DataFrame."
    
    rel_date=movie['release_date'].iloc[0]
    popty=movie['popularity'].iloc[0]
    rel_year=rel_date.year
    
    
    return {f" La pelicula '{t}' fue estrenada en el año {rel_year} con una popularidad de {popty}"}

@app.get("/score_titulo/{title}")
def get_score_titulo(title: str):
    try:
        s=score_titulo(df, title)
        return {"message": s}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def votos_titulo(df, tl):

    mo=df[df['title']==tl]
    
    if mo.empty:
        return f"La película '{tl}' no se encuentra en el DataFrame."
    
    if mo['vote_count'].iloc[0]>2000:
        i=mo[['vote_count', 'vote_average', 'release_date']].iloc[0]
        return i
    else:
        return f"La película '{tl}' no cumple con la condición de tener más de 2000 votos."

@app.get("/votos_titulo/{title}")
def get_votos_titulo(title: str):
    try:
        u=votos_titulo(df, title)
        if isinstance(u, str):
            raise HTTPException(status_code=404, detail=u)
        return {"message": "Película encontrada", "data": u}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


df['id']=df['id'].astype(str)
df3['cast_id']=df3['cast_id'].astype(str)

def get_actor(df, df3, actor):
    
    df_joined=df3.merge(df, left_on='cast_id', right_on='id')
    
    movies_actor=df_joined[df_joined['actor_name']==actor]
    
    if movies_actor.empty:
        return f"No se encontraron películas para el actor '{actor}'."
    
    total_return=movies_actor['return'].sum()
    
    numer_of_movies=movies_actor.shape[0]
    
    average_return=movies_actor['return'].mean()
    
    
    return f"El actor '{actor}' ha generado un total de {total_return:.2f} en 'return', ha participado en {numer_of_movies} películas, con un promedio de 'return' de {average_return:.2f}."

@app.get("/get_actor/{actor}")
def fetch_actor(actor: str):
    try:
        l=get_actor(df, df3, actor)
        return {"message": l}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


def get_director(df2, df, director):
 
    df2=df2[(df2['job']=='director')&(df2['crew_name']==director)]

    df_merged=pd.merge(df2, df, left_on='credit_id', right_on='id', how='inner')

    num_movies=df_merged.shape[0]

    total_return=df_merged['return'].sum()

    average_return=df_merged['return'].mean() if num_movies>0 else 0

    return num_movies, total_return, average_return

@app.get("/get_director/{director}")
def fetch_director(director: str):
    try:
        num_movies, total_return, average_return = get_director(df2, df, director)
        return {"message": f"Director: {director}, Películas: {num_movies}, Retorno Total: {total_return:.2f}, Retorno Promedio: {average_return:.2f}"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
