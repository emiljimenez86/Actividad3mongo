import pymongo
from pymongo import MongoClient

# Configuración de MongoDB
MONGO_HOST = "localhost"
MONGO_PUERTO = "27017"
MONGO_URL = f"mongodb://{MONGO_HOST}:{MONGO_PUERTO}/"
MONGO_BASEDATOS = "Torneo"

# Conexión a MongoDB
try:
    Torneo = MongoClient(MONGO_URL, serverSelectionTimeoutMS=2000)
    Torneo.server_info()  # Verifica conexión
    print("Conexión exitosa a MongoDB")
except pymongo.errors.ServerSelectionTimeoutError as error_tiempo:
    print("Error de conexión: Tiempo excedido")
    exit()
except pymongo.errors.ConnectionFailure as error_conexion:
    print("Error de conexión: Fallo al conectarse")
    exit()

# Seleccionar la base de datos
db = Torneo[MONGO_BASEDATOS]

# Leer datos de la colección grande
Equipos = db.Equipos.find()
total_documentos = db.Equipos.count_documents({})
procesados = 0

for Torneo in Equipos:
    ubicacion = Torneo.get("Ubicacion")
    try:
        if ubicacion == "Sur":
            db.Equipo_Sur.insert_one(Torneo)
        elif ubicacion == "Norte":
            db.Equipo_Norte.insert_one(Torneo)
        else:
            print(f"Documento ignorado: Ubicación no válida - {Torneo}")
    except pymongo.errors.PyMongoError as e:
        print(f"Error al insertar documento: {Torneo}, Error: {e}")
    
    procesados += 1
    if procesados % 100 == 0:
        print(f"{procesados}/{total_documentos} documentos procesados.")

# Confirmar y eliminar la colección grande
conteo_sur = db.Equipo_Sur.count_documents({})
print(conteo_sur)
conteo_norte = db.Equipo_Norte.count_documents({})
print(conteo_norte)
if procesados == total_documentos:
    db.Equipos.drop()
    print("Colección grande eliminada exitosamente.")
else:
    print("Los documentos no coinciden; no se eliminó la colección grande.")