import pandas as pd
import mysql.connector
import numpy as np
from fastapi import FastAPI
from starlette.responses import RedirectResponse

aplicacion = FastAPI()

#############
@aplicacion.get("/")
async def raiz():
    return RedirectResponse(url="/docs")

@aplicacion.get("/duracion/")
async def get_max_duration(plataforma:str,año:str,tiempo:str):
    mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
    cursor = mydb.cursor()
    cursor.execute("USE services;")
    sql = 'SELECT title FROM'+' '+str(plataforma)+' '+'WHERE release_year ='+' '+str(año)+' '+'AND duration_show ='+' '+'"'+'min'+'"'+' '+'ORDER BY duration DESC LIMIT 1'
    cursor.execute(sql)
    consulta = cursor.fetchall()
    cursor.close()
    return {"El show de mayor duracion es: ": str(consulta)}

@aplicacion.get("/consulta/")
def get_actor(plataforma:str,year:str):
    mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
    cursor = mydb.cursor()
    cursor.execute("USE services;")
    sql = 'SELECT cast FROM'+' '+str(plataforma)+' '+'WHERE release_year = '+' '+str(year)+' and cast !='+'"'+'Sin dato'+'"'
    cursor.execute(sql)
    consulta = cursor.fetchall()
    cursor.close()
    return {"El Actor que mas se repite es: " :str(consulta)}

@aplicacion.get("/listado_en")
def get_listedin(listed_in:str,plataforma:str):
    mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
    cursor = mydb.cursor()
    cursor.execute("USE services;")
    sql = 'SELECT listed_in,count(listed_in) FROM'+' '+str(plataforma)+' '+'WHERE listed_in like'+' '+'"'+'%'+str(listed_in)+'%'+'"'
    cursor.execute(sql)
    consulta = cursor.fetchall()
    return {"El show listado en segun plataforma: ":str(consulta)}

@aplicacion.get("/cuenta")
def get_count_plataform(plataforma:str):
    mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
    cursor = mydb.cursor()
    cursor.execute("USE services;")
    sql = 'SELECT type,count(type) as cuenta_de_show FROM'+' '+str(plataforma)+' '+'GROUP BY type'
    cursor.execute(sql)
    consulta = cursor.fetchall()
    return {"La cuenta segun la plataforma es: ":str(consulta)}
