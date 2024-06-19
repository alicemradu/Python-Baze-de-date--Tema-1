### 1.1. IMPORT FISIER .CSV
import pandas as pd
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table, Numeric
from sqlalchemy.dialects.oracle import FLOAT as OracleFLOAT
from sqlalchemy import distinct
import oracledb
import warnings
warnings.filterwarnings('ignore', r".*Did not recognize type 'SDO_GEOMETRY' of column.*", SAWarning)

# Configurare conexiune BD
username = 'RADUA_30'
password = 'STUD'
host = '193.226.34.57'
port = '1521'
service_name = 'orclpdb.docker.internal'

# Creare conexiune
engine = create_engine(f'oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}')

# Citire fisier .csv
csv_file = r'D:\Important\Alice\Fuckultate\Anul 1\Semestrul 2\Managementul bazelor de date\pythonProject\data\movies.csv'
df = pd.read_csv(csv_file)

# Date tabel
metadata = MetaData()

movies_table = Table(
    "Movies",
    metadata,
    Column("name", String(255)),
    Column("genre", String(255)),
    Column("year", Integer),
    Column("score", OracleFLOAT(binary_precision=53)),
    Column("votes", Integer),
    Column("director", String(255)),
    Column("writer", String(255)),
    Column("star", String(255)),
    Column("country", String(255)),
    Column("budget", Numeric(20, 2)),
    Column("gross", Numeric(20, 2)),
    Column("company", String(255)),
    Column("runtime", Integer),
)

# Create tabel
metadata.create_all(engine)

# Salvează date în DB
df.to_sql('Movies', con=engine, if_exists='replace', index=False, dtype={
    "name": String(255),
    "genre": String(255),
    "year": Integer,
    "score": OracleFLOAT(binary_precision=53),
    "votes": Integer,
    "director": String(255),
    "writer": String(255),
    "star": String(255),
    "country": String(255),
    "budget": Numeric(20, 2),
    "gross": Numeric(20, 2),
    "company": String(255),
    "runtime": Integer,
})

# Creare sesiune
Session = sessionmaker(bind=engine)
session = Session()

# Load the table from metadata
metadata.reflect(bind=engine)

movies_table = metadata.tables['Movies']

### 1.2. INTEROGARI SI PRELUCRARI ASUPRA TABELEI

print('############# INTEROGARI SI PRELUCRARI ASUPRA TABELEI #############')

# Interogare 1: Selectare filmele dintr-un anumit gen
query1 = session.query(movies_table).filter_by(genre='Thriller').all()
print("Interogare 1: Filme din genul 'Thriller'")
for movie in query1:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 2: Selectare filme dintr-un anumit an cu un scor mai mare de 8
query2 = session.query(movies_table).filter_by(year=2018).filter(movies_table.c.score > 8).all()
print("\nInterogare 2: Filme din anul 2018 cu scor mai mare de 8")
for movie in query2:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 3: Calculul mediei scorurilor pentru filmele dintr-un anumit an
from sqlalchemy import func
query3 = session.query(func.avg(movies_table.c.score)).filter_by(year=2019).scalar()
print(f"\nInterogare 3: Media scorurilor pentru filmele din anul 2019: {query3}")

# Interogare 4: Selectare filme regizate de un anumit regizor
query4 = session.query(movies_table).filter_by(director='Christopher Nolan').all()
print("\nInterogare 4: Filme regizate de 'Christopher Nolan'")
for movie in query4:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 5: Afisarea primelor 5 filme ordonate dupa numarul de voturi
query5 = session.query(movies_table).order_by(movies_table.c.votes.desc()).limit(5).all()
print("\nInterogare 5: Primele 5 filme ordonate după numarul de voturi")
for movie in query5:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 6: Count filme pentru fiecare tara
query6 = session.query(movies_table.c.country, func.count('*')).group_by(movies_table.c.country).all()
print("\nInterogare 6: Count filme pentru fiecare tara")
for country, count in query6:
    print(f"{country}: {count} filme")

# Interogare 7: Calculul sumei incasarilor totale pentru filmele dintr-un anumit gen
query7 = session.query(func.sum(movies_table.c.gross)).filter_by(genre='Comedy').scalar()
print(f"\nInterogare 7: Suma incasarilor totale pentru filmele din genul 'Comedy': {query7}")

# Interogare 8: Selectare filme care au bugetul minim 300 milioane
query8 = session.query(movies_table).filter(movies_table.c.budget >= 300000000).all()
print("\nInterogare 8: Filme cu un buget de minim 300 milioane")
for movie in query8:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 9: Actualizare scor pentru un anumit film
updated_rows = session.query(movies_table).filter_by(name='Inception').update({movies_table.c.score: 9.2})
session.commit()

if updated_rows > 0:
    print("\nInterogare 9: Scorul pentru filmul 'Inception' a fost actualizat cu succes la 9.2!")
else:
    print("\nInterogare 9: Nu a fost gasit filmul 'Inception' pentru actualizare")


# Interogare 10: Sterge toate filmele cu voturi mai puțin de 1000
deleted_rows = session.query(movies_table).filter(movies_table.c.votes < 1000).delete()
session.commit()

if deleted_rows > 0:
    print("\nInterogare 10: Filmele cu mai putin de 1000 de voturi au fost sterse cu succes!")
else:
    print("\nInterogare 10: Nu au fost gasite filme cu mai putin de 1000 voturi!\n")

### 2. APLICAREA ALTOR OPERATORI/FUNCTII SQL IN SQL ALCHEMY

print('############# APLICAREA ALTOR OPERATORI/FUNCTII SQL IN SQL ALCHEMY #############')

# Interogare 11: Count nr filme regizate pentru fiecare regizor --COUNT
query11 = session.query(movies_table.c.director, func.count('*').label('numar_filme')).group_by(movies_table.c.director).order_by(func.count('*').desc()).limit(3).all()
print("\nInterogare 11: Primii 3 regizori cu cele mai multe filme")
for director, numar_filme in query11:
    print(f"{director}: {numar_filme} filme")

# Interogare 12: Afisare film cu cel mai mare buget --MAX
max_budget = session.query(func.max(movies_table.c.budget)).scalar()
query12 = session.query(movies_table).filter(movies_table.c.budget == max_budget).all()
print("\nInterogare 12: Filmul cu cel mai mare buget este: ")
for movie in query12:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 13: Filmul cu cel mai mic scor --MIN
min_score = session.query(func.min(movies_table.c.score)).scalar()
query13 = session.query(movies_table).filter(movies_table.c.score == min_score).all()
print("\nInterogare 13: Filmul cu cel mai mic scor este: ")
for movie in query13:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 14: Primele 3 caractere ale fiecarui gen de filme --SUBSTRING
query14 = session.query(func.substr(movies_table.c.genre, 1, 3).label('genre_prefix')).distinct().all()
print("\nInterogare 14: Primele 3 caractere ale fiecărui gen de filme:")
for genre_prefix, in query14:
    print(genre_prefix)

# Interogare 15: Afisare filme al caror nume contine mai mult de 10 caractere -LENGTH
query15 = session.query(movies_table).filter(func.length(movies_table.c.name) > 10).limit(10).all()
print("\nInterogare 15: Primele 10 filme cu numele mai lung de 10 caractere: ")
for movie in query15:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 16: Concatenează coloanele `director` și `writer`
query16 = session.query(movies_table.c.name, (movies_table.c.director + " / " + movies_table.c.writer).label('director_writer')).limit(5).all()
print("\nInterogare 16: Primele 5 filme cu coloanele `director` și `writer` concatenate")
for name, director_writer in query16:
    print(f"{name}: {director_writer}")

# Interogare 17: Rotunjeste scorurile filmelor la 1 zecimala --ROUND
query17 = session.query(movies_table.c.name, func.round(movies_table.c.score, 1).label('rounded_score')).limit(10).all()
print("\nInterogare 17: Primele 10 filme cu scorurile rotunjite la 1 zecimală")
for name, rounded_score in query17:
    print(f"{name}: {rounded_score}")

# Interogare 18: Calcularea diferentei dintre buget si incasari
query18 = session.query(movies_table.c.name, (movies_table.c.gross - movies_table.c.budget).label('profit')).limit(5).all()
print("\nInterogare 18: Primele 5 filme cu diferența dintre buget și încasări (profit)")
for name, profit in query18:
    print(f"{name}: {profit}")

# Interogare 19: Selectarea filmelor care conțin cuvantul 'War' in titlu --LIKE
query19 = session.query(movies_table).filter(movies_table.c.name.like('%War%')).all()
print("\nInterogare 19: Filme care conțin cuvantul 'War' in titlu:")
for movie in query19:
    movie_dict = {column.name: getattr(movie, column.name) for column in movies_table.columns}
    print(movie_dict)

# Interogare 20: Calcularea duratei medii a filmelor din genul 'Drama'
query20 = session.query(func.avg(movies_table.c.runtime)).filter_by(genre='Drama').scalar()
print(f"\nInterogare 20: Durata medie a filmelor din genul 'Drama': {query20}")

### 3. OPERATII ACTUALIZARE PE TABELA MOVIES

# Actualizare scor pentru un anumit film
session.query(movies_table).filter_by(name='Inception').update({movies_table.c.score: 9.2})
session.commit()
print("Scorul pentru filmul 'Inception' a fost actualizat cu succes la 9.2!")

# Actualizare gen pentru un anumit film
session.query(movies_table).filter_by(name='Inception').update({movies_table.c.genre: 'Sci-Fi'})
session.commit()
print("Genul pentru filmul 'Inception' a fost actualizat cu succes la 'Sci-Fi'!")


# Actualizare buget și încasări pentru un anumit film
session.query(movies_table).filter_by(name='Avatar').update({movies_table.c.budget: 250000000, movies_table.c.gross: 2787965087})
session.commit()
print("Bugetul si incasarile pentru filmul 'Avatar' au fost actualizate cu succes!")


# Actualizare voturi pentru un anumit film
session.query(movies_table).filter_by(name='Inception').update({movies_table.c.votes: 1000000})
session.commit()
print("Numărul de voturi pentru filmul 'Inception' a fost actualizat cu succes la 1.000.000!")

# Actualizare durată pentru un anumit film
session.query(movies_table).filter_by(name='Inception').update({movies_table.c.runtime: 160})
session.commit()
print("Durata pentru filmul 'Inception' a fost actualizată cu succes la 160 de minute!")

# Actualizare țară pentru filmele cu un anumit regizor
session.query(movies_table).filter(movies_table.c.director == 'Steven Spielberg').update({movies_table.c.country: 'USA'})
session.commit()
print("Țara pentru filmele regizate de Steven Spielberg a fost actualizată cu succes la 'USA'!")

# Actualizare buget pentru filmele cu un anumit interval de ani
session.query(movies_table).filter(movies_table.c.year.between(2010, 2020)).update({movies_table.c.budget: movies_table.c.budget * 1.1})
session.commit()
print("Bugetul pentru filmele lansate între 2010 și 2020 a fost actualizat cu succes!")
