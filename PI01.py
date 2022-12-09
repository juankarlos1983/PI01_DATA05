import pandas as pd
import mysql.connector
import numpy as np

class PI01_DATA05:

    def __init__(self):
        pass
    
    def normalizar(self):
        self.amazon = pd.read_csv("Datasets/amazon_prime_titles.csv")
        self.disney = pd.read_csv("Datasets/disney_plus_titles.csv")
        self.hulu = pd.read_csv("Datasets/hulu_titles.csv")
        self.netflix = pd.read_json("Datasets/netflix_titles.json")
        #Crea una nueva columna para separar min y seasons, y asi poder trabajor 
        # en la columna duration con numero enteros
        self.amazon['duration_show'] = np.nan
        self.disney['duration_show'] = np.nan
        self.hulu['duration_show'] = np.nan
        self.netflix['duration_show'] = np.nan
        #Reemplazar el valor NaN por; 'Sin dato', en todas tablas
        #Tabla Amazon
        self.amazon['type'] = self.amazon['type'].replace([np.nan],['Sin dato'])
        self.amazon['title'] = self.amazon['title'].replace([np.nan],['Sin dato'])
        self.amazon['director'] = self.amazon['director'].replace([np.nan],['Sin dato'])
        self.amazon['cast'] = self.amazon['cast'].replace([np.nan],['Sin dato'])
        self.amazon['country'] = self.amazon['country'].replace([np.nan],['Sin dato'])
        self.amazon['date_added'] = self.amazon['date_added'].replace([np.nan],['Sin dato'])
        self.amazon['release_year'] = self.amazon['release_year'].replace([np.nan],['Sin dato'])
        self.amazon['rating'] = self.amazon['rating'].replace([np.nan],['Sin dato'])
        self.amazon['duration'] = self.amazon['duration'].replace([np.nan],['Sin dato'])
        self.amazon['listed_in'] = self.amazon['listed_in'].replace([np.nan],['Sin dato'])
        self.amazon['description'] = self.amazon['description'].replace([np.nan],['Sin dato'])
        self.amazon['duration_show'] = self.amazon['duration_show'].replace([np.nan],['Sin dato'])
        #Tabla disney
        self.disney['type'] = self.disney['type'].replace([np.nan],['Sin dato'])
        self.disney['title'] = self.disney['title'].replace([np.nan],['Sin dato'])
        self.disney['director'] = self.disney['director'].replace([np.nan],['Sin dato'])
        self.disney['cast'] = self.disney['cast'].replace([np.nan],['Sin dato'])
        self.disney['country'] = self.disney['country'].replace([np.nan],['Sin dato'])
        self.disney['date_added'] = self.disney['date_added'].replace([np.nan],['Sin dato'])
        self.disney['release_year'] = self.disney['release_year'].replace([np.nan],['Sin dato'])
        self.disney['rating'] = self.disney['rating'].replace([np.nan],['Sin dato'])
        self.disney['duration'] = self.disney['duration'].replace([np.nan],['Sin dato'])
        self.disney['listed_in'] = self.disney['listed_in'].replace([np.nan],['Sin dato'])
        self.disney['description'] = self.disney['description'].replace([np.nan],['Sin dato'])
        self.disney['duration_show'] = self.disney['duration_show'].replace([np.nan],['Sin dato'])
        #Tabla Hulu
        self.hulu['type'] = self.hulu['type'].replace([np.nan],['Sin dato'])
        self.hulu['title'] = self.hulu['title'].replace([np.nan],['Sin dato'])
        self.hulu['director'] = self.hulu['director'].replace([np.nan],['Sin dato'])
        self.hulu['cast'] = self.hulu['cast'].replace([np.nan],['Sin dato'])
        self.hulu['country'] = self.hulu['country'].replace([np.nan],['Sin dato'])
        self.hulu['date_added'] = self.hulu['date_added'].replace([np.nan],['Sin dato'])
        self.hulu['release_year'] = self.hulu['release_year'].replace([np.nan],['Sin dato'])
        self.hulu['rating'] = self.hulu['rating'].replace([np.nan],['Sin dato'])
        self.hulu['duration'] = self.hulu['duration'].replace([np.nan],['Sin dato'])
        self.hulu['listed_in'] =self.hulu['listed_in'].replace([np.nan],['Sin dato'])
        self.hulu['description'] = self.hulu['description'].replace([np.nan],['Sin dato'])
        self.hulu['duration_show'] = self.hulu['duration_show'].replace([np.nan],['Sin dato'])
        #Tabla Netflix
        self.netflix['type'] = self.netflix['type'].replace([np.nan],['Sin dato'])
        self.netflix['title'] = self.netflix['title'].replace([np.nan],['Sin dato'])
        self.netflix['director'] = self.netflix['director'].replace([np.nan],['Sin dato'])
        self.netflix['cast'] = self.netflix['cast'].replace([np.nan],['Sin dato'])
        self.netflix['country'] =self.netflix['country'].replace([np.nan],['Sin dato'])
        self.netflix['date_added'] = self.netflix['date_added'].replace([np.nan],['Sin dato'])
        self.netflix['release_year'] = self.netflix['release_year'].replace([np.nan],['Sin dato'])
        self.netflix['rating'] = self.netflix['rating'].replace([np.nan],['Sin dato'])
        self.netflix['duration'] = self.netflix['duration'].replace([np.nan],['Sin dato'])
        self.netflix['listed_in'] = self.netflix['listed_in'].replace([np.nan],['Sin dato'])
        self.netflix['description'] = self.netflix['description'].replace([np.nan],['Sin dato'])
        self.netflix['duration_show'] = self.netflix['duration_show'].replace([np.nan],['Sin dato'])
        ##########################################################################################################################################
        #En este codigo pasamos la duracion del show (en min o seasons) que se encuentra en la columna rating y la pasamos a la columna duration
        #Esto solo se encontro en hulu y netflix
        for index, row in self.hulu.iterrows():
            try:
                if row.rating.split()[1] in ['min','Seasons','Season']:
                    self.hulu.at[index,'duration'] = row.rating
                    self.hulu.at[index,'rating'] = 'Sin dato'
            except:
                pass
        for index, row in self.netflix.iterrows():
            try:
                if 'min' in row.rating:
                    self.netflix.at[index,'duration'] = row.rating
                    self.netflix.at[index,'rating'] = 'Sin dato'
            except:
                pass
        ########################################################################################################
        #Separar min y seasons, y asi poder trabajar en la columna duration con numero enteros.
        for index, row in self.amazon.iterrows():
            if row.duration != 'Sin dato':
                self.amazon.at[index,'duration_show'] = row.duration.split()[1]
                self.amazon.at[index,'duration'] = int(row.duration.split()[0])
            else:
                self.amazon.at[index,'duration'] = 0
        for index, row in self.disney.iterrows():
            if row.duration != 'Sin dato':
                self.disney.at[index,'duration_show'] = row.duration.split()[1]
                self.disney.at[index,'duration'] = int(row.duration.split()[0])
            else:
                self.disney.at[index,'duration'] = 0
        for index, row in self.hulu.iterrows():
            if row.duration != 'Sin dato':
                self.hulu.at[index,'duration_show'] = row.duration.split()[1]
                self.hulu.at[index,'duration'] = int(row.duration.split()[0])
            else:
                self.hulu.at[index,'duration'] = 0
        for index, row in self.netflix.iterrows():
            if row.duration != 'Sin dato':
                self.netflix.at[index,'duration_show'] = row.duration.split()[1]
                self.netflix.at[index,'duration'] = int(row.duration.split()[0])
            else:
                self.netflix.at[index,'duration'] = 0

    def conn_base_datos(self):
        #Conectar con la base de datos
        self.mydb = mysql.connector.connect(host="localhost", user="root", password="123456")
        self.cursor = self.mydb.cursor()
        

    def crear_tablas(self):
        self.conn_base_datos()
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS services;")
        self.cursor.execute("USE services;")
        tablas =  '''
        CREATE TABLE IF NOT EXISTS amazon (
            show_id varchar(10),
            type varchar(200),
            title varchar(200),
            director TEXT,
            cast TEXT,
            country varchar(200),
            date_added varchar(200),
            release_year int,
            rating varchar(200),
            duration int,
            duration_show varchar(20),
            listed_in varchar(200),
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS disney (
            show_id varchar(10),
            type varchar(200),
            title varchar(200),
            director TEXT,
            cast TEXT,
            country varchar(200),
            date_added varchar(200),
            release_year int,
            rating varchar(200),
            duration int,
            duration_show varchar(20),
            listed_in varchar(200),
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS hulu (
            show_id varchar(10),
            type varchar(200),
            title varchar(200),
            director TEXT,
            cast TEXT,
            country varchar(200),
            date_added varchar(200),
            release_year int,
            rating varchar(200),
            duration int,
            duration_show varchar(20),
            listed_in varchar(200),
            description TEXT
        );
        CREATE TABLE IF NOT EXISTS netflix (
            show_id varchar(10),
            type varchar(200),
            title varchar(200),
            director TEXT,
            cast TEXT,
            country varchar(200),
            date_added varchar(200),
            release_year int,
            rating varchar(200),
            duration int,
            duration_show varchar(20),
            listed_in varchar(200),
            description TEXT
        )
        '''
        self.cursor.execute(tablas)
        self.cursor.close()

    def ingestar_datos(self):
        self.conn_base_datos()
        
        amazonsql = "INSERT INTO services.amazon(show_id,type,title,director,cast,country,date_added,release_year,rating,duration,duration_show,listed_in,description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for index,row in self.amazon.iterrows():
            self.cursor.execute(amazonsql,(row.show_id,row.type,row.title,row.director,row.cast,row.country,row.date_added,row.release_year,row.rating,row.duration,row.duration_show,row.listed_in,row.description))
            self.mydb.commit()
    
        disneysql = "INSERT INTO services.disney(show_id,type,title,director,cast,country,date_added,release_year,rating,duration,duration_show,listed_in,description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"    
        for index,row in self.disney.iterrows():
            self.cursor.execute(disneysql,(row.show_id,row.type,row.title,row.director,row.cast,row.country,row.date_added,row.release_year,row.rating,row.duration,row.duration_show,row.listed_in,row.description))
            self.mydb.commit()

        hulusql = "INSERT INTO services.hulu(show_id,type,title,director,cast,country,date_added,release_year,rating,duration,duration_show,listed_in,description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"    
        for index,row in self.hulu.iterrows():
            self.cursor.execute(hulusql,(row.show_id,row.type,row.title,row.director,row.cast,row.country,row.date_added,row.release_year,row.rating,row.duration,row.duration_show,row.listed_in,row.description))
            self.mydb.commit()

        netflixsql = "INSERT INTO services.netflix(show_id,type,title,director,cast,country,date_added,release_year,rating,duration,duration_show,listed_in,description) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"    
        for index,row in self.netflix.iterrows():
            self.cursor.execute(netflixsql,(row.show_id,row.type,row.title,row.director,row.cast,row.country,row.date_added,row.release_year,row.rating,row.duration,row.duration_show,row.listed_in,row.description))
            self.mydb.commit()
        self.cursor.close()



    def get_actor(self,plataforma,year,cast = 'Sin dato'):
        self.conn_base_datos()
        self.cursor.execute("USE services;")
        sql = 'SELECT cast FROM'+' '+plataforma+' '+'WHERE release_year = '+' '+year+' and cast !='+'"'+cast+'"'
        self.cursor.execute(sql)
        consulta = self.cursor.fetchall()
        self.cursor.close()
        return consulta
    def get_listedin(self,listed_in,plataforma = 'amazon'):
        self.conn_base_datos()
        self.cursor.execute("USE services;")
        sql = 'SELECT listed_in,count(listed_in) FROM'+' '+plataforma+' '+'WHERE listed_in like'+' '+'"'+'%'+listed_in+'%'+'"'
        self.cursor.execute(sql)
        consulta = self.cursor.fetchall()
        return consulta
    def get_count_plataform(self,plataforma = 'netflix'):
        self.conn_base_datos()
        self.cursor.execute("USE services;")
        sql = 'SELECT type,count(type) as cuenta_de_show FROM'+' '+plataforma+' '+'GROUP BY type'
        self.cursor.execute(sql)
        consulta = self.cursor.fetchall()
        return consulta
    def get_max_duration(self,year,plataforma,tiempo):
        self.conn_base_datos()
        self.cursor.execute("USE services;")
        sql = 'SELECT title FROM'+' '+plataforma+' '+'WHERE release_year ='+' '+year+' '+'AND duration_show ='+' '+'"'+tiempo+'"'+' '+'ORDER BY duration DESC LIMIT 1'
        self.cursor.execute(sql)
        consulta = self.cursor.fetchall()
        return consulta



PI = PI01_DATA05()
#PI.normalizar()
#PI.crear_tablas()
#PI.ingestar_datos()
#print(PI.get_actor('netflix','2018'))
#print(PI.get_listedin('comedy'))
#print(PI.get_count_plataform())
#print(PI.get_max_duration('2018','hulu','min'))
