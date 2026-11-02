import os
import re
import math
import socket
import json
import time
import threading
import urllib.parse
import urllib.request
import mysql.connector
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO

# Carga las variables del archivo .env que esta junto a app.py.
# Cada instancia EC2 tiene su propio .env (nunca se sube a GitHub).
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

NOMBRE_PAGINA = os.getenv('NOMBRE_PAGINA', 'Telemetría GPS en Tiempo Real')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
UDP_PORT = int(os.getenv('UDP_PORT', '5000'))
WEB_PORT = int(os.getenv('WEB_PORT', '80'))

# --- Historico: zonas horarias y tope de puntos por consulta ---
# MySQL guarda en UTC; el usuario piensa en hora de Colombia (UTC-5).
BOGOTA = ZoneInfo('America/Bogota')
UTC = ZoneInfo('UTC')
LIMITE_PUNTOS = 5000
# Margen despues de FIN_RECORRIDO para aceptar el ultimo punto del viaje
# (el telefono puede enviar un punto justo despues de la marca de fin).
TOLERANCIA_FIN_VIAJE = timedelta(seconds=15)

# --- Vehiculos: la app de cada celular manda "Vehiculo: N" en el paquete ---
# Un paquete sin esa linea (app vieja) se asigna al vehiculo 1.
VEHICULOS = (1, 2)
VEHICULO_POR_DEFECTO = 1

# --- Lugar: radio de busqueda, fijo en el codigo (el usuario no lo elige) ---
RADIO_LUGAR_METROS = 200

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

def separar_fecha_hora(valor):
    try:
        fecha_str, hora_str = valor.strip().split(' ')
        a, m, d = fecha_str.split('-')
        return f"{d}/{m}/{a}", hora_str.split('.')[0]
    except Exception:
        return valor, valor

def conectar_mysql():
    """Abre una conexion a MySQL con los datos del .env."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connection_timeout=5
    )

# --- Vehiculos: ¿las tablas ya tienen la columna "vehiculo"? ---
# La migracion SQL (docs/MIGRACION_VEHICULOS.md) agrega la columna. Mientras
# una instancia no la tenga, la pagina sigue funcionando solo con el vehiculo 1
# (los datos del vehiculo 2 se muestran en vivo pero NO se guardan, para no
# mezclarlos con los del vehiculo 1). Una vez detectada la columna se recuerda;
# si no existe, se vuelve a revisar cada 30 s.
_cache_columna_vehiculo = {}

def tabla_tiene_vehiculo(tabla):
    assert tabla in ('ubicaciones', 'viajes')
    ahora = time.time()
    previo = _cache_columna_vehiculo.get(tabla)
    if previo and (previo[0] or ahora - previo[1] < 30):
        return previo[0]
    tiene = False
    try:
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {tabla} LIKE 'vehiculo'")
        tiene = cursor.fetchone() is not None
        cursor.close()
        conexion.close()
    except Exception as error:
        print(f"Aviso: no se pudo revisar la columna vehiculo de {tabla}: {error}")
    _cache_columna_vehiculo[tabla] = (tiene, ahora)
    return tiene

def extraer_vehiculo(lineas):
    """Lee la linea "Vehiculo: N" de un paquete. Devuelve VEHICULO_POR_DEFECTO
    si no viene, el numero si es valido, o None si viene pero no es valido."""
    for linea in lineas:
        m = re.match(r'^\s*Veh[ií]culo\s*:\s*(\d+)\s*$', linea, re.IGNORECASE)
        if m:
            numero = int(m.group(1))
            return numero if numero in VEHICULOS else None
    return VEHICULO_POR_DEFECTO

def guardar_en_mysql(datos):
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        print("Faltan variables de base de datos en el .env. No se guarda el registro.")
        return
    vehiculo = datos['vehiculo']
    try:
        if tabla_tiene_vehiculo('ubicaciones'):
            consulta = """
                INSERT INTO ubicaciones (latitud, longitud, fecha, hora_col, vehiculo)
                VALUES (%s, %s, %s, %s, %s)
            """
            valores = (datos['latitud'], datos['longitud'], datos['fecha'], datos['hora_col'], vehiculo)
        elif vehiculo == VEHICULO_POR_DEFECTO:
            consulta = """
                INSERT INTO ubicaciones (latitud, longitud, fecha, hora_col)
                VALUES (%s, %s, %s, %s)
            """
            valores = (datos['latitud'], datos['longitud'], datos['fecha'], datos['hora_col'])
        else:
            print(f"Aviso: falta la columna vehiculo en ubicaciones; no se guarda el vehiculo {vehiculo}.")
            return
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(consulta, valores)
        conexion.commit()
        cursor.close()
        conexion.close()
        print(f"--> Registro del vehiculo {vehiculo} guardado en MySQL (AWS RDS) exitosamente.")
    except Exception as error:
        print(f"Error al conectar/guardar en MySQL: {error}")

# --- Viajes: marcas de inicio y fin de recorrido ---
# Se disparan cuando la app Android manda los mensajes cortos
# "INICIO_RECORRIDO" / "FIN_RECORRIDO" (con la linea "Vehiculo: N" debajo),
# justo cuando el usuario presiona los botones Iniciar/Finalizar. No se
# infieren los limites del viaje revisando huecos de tiempo: el propio
# usuario confirma cuando empieza y cuando termina. Cada vehiculo tiene sus
# propios viajes: el FIN del vehiculo 2 nunca cierra el viaje del vehiculo 1.
def registrar_inicio_viaje(vehiculo):
    """Crea una fila nueva en viajes al recibir INICIO_RECORRIDO. fin queda NULL."""
    try:
        if tabla_tiene_vehiculo('viajes'):
            consulta, valores = "INSERT INTO viajes (inicio, vehiculo) VALUES (NOW(), %s)", (vehiculo,)
        elif vehiculo == VEHICULO_POR_DEFECTO:
            consulta, valores = "INSERT INTO viajes (inicio) VALUES (NOW())", ()
        else:
            print(f"Aviso: falta la columna vehiculo en viajes; no se registra el viaje del vehiculo {vehiculo}.")
            return
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(consulta, valores)
        conexion.commit()
        cursor.close()
        conexion.close()
        print(f"--> Viaje del vehiculo {vehiculo} iniciado.")
    except Exception as error:
        print(f"Error al registrar inicio de viaje: {error}")

def registrar_fin_viaje(vehiculo):
    """Cierra el viaje abierto mas reciente (fin IS NULL) de ese vehiculo."""
    try:
        if tabla_tiene_vehiculo('viajes'):
            consulta = """
                UPDATE viajes SET fin = NOW()
                WHERE fin IS NULL AND vehiculo = %s
                ORDER BY inicio DESC
                LIMIT 1
            """
            valores = (vehiculo,)
        elif vehiculo == VEHICULO_POR_DEFECTO:
            consulta = """
                UPDATE viajes SET fin = NOW()
                WHERE fin IS NULL
                ORDER BY inicio DESC
                LIMIT 1
            """
            valores = ()
        else:
            return
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(consulta, valores)
        conexion.commit()
        filas_afectadas = cursor.rowcount
        cursor.close()
        conexion.close()
        if filas_afectadas == 0:
            print(f"--> FIN_RECORRIDO del vehiculo {vehiculo} recibido, pero no habia ningun viaje abierto.")
        else:
            print(f"--> Viaje del vehiculo {vehiculo} finalizado.")
    except Exception as error:
        print(f"Error al registrar fin de viaje: {error}")

def coordenadas_validas(lat_texto, lon_texto):
    """True si son numeros reales dentro de rango y no el (0, 0) de un GPS sin senal.
    Descarta ruido de la red (por ejemplo, escaneos SIP al puerto UDP)."""
    try:
        lat = float(lat_texto)
        lon = float(lon_texto)
    except ValueError:
        return False
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False
    return not (lat == 0 and lon == 0)

def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', UDP_PORT))
    print(f"Escuchando mensajes UDP en el puerto {UDP_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        try:
            mensaje = data.decode('utf-8').strip()
        except UnicodeDecodeError:
            continue

        lineas = mensaje.split('\n')

        # Marcas de inicio/fin de viaje: la primera linea es la marca y, si
        # viene, la segunda dice a que vehiculo pertenece. Se revisan primero
        # para no intentar interpretarlas como una ubicacion.
        marca = lineas[0].strip()
        if marca in ("INICIO_RECORRIDO", "FIN_RECORRIDO"):
            vehiculo = extraer_vehiculo(lineas[1:])
            if vehiculo is None:
                print(f"Marca descartada: vehiculo invalido desde {addr[0]}.")
                continue
            if marca == "INICIO_RECORRIDO":
                registrar_inicio_viaje(vehiculo)
            else:
                registrar_fin_viaje(vehiculo)
            continue

        if len(lineas) >= 5:
            lat_texto = lineas[1].replace('Latitud: ', '').strip()
            lon_texto = lineas[2].replace('Longitud: ', '').strip()
            vehiculo = extraer_vehiculo(lineas[5:])

            if vehiculo is None or not coordenadas_validas(lat_texto, lon_texto):
                print(f"Paquete descartado (no es una ubicacion valida) desde {addr[0]}.")
                continue

            crudo_col = lineas[4].replace('Hora Colombia: ', '').strip()
            fecha, hora_col = separar_fecha_hora(crudo_col)

            datos_json = {
                'latitud': lat_texto,
                'longitud': lon_texto,
                'fecha': fecha,
                'hora_col': hora_col,
                'vehiculo': vehiculo
            }

            socketio.emit('actualizacion_gps', datos_json)
            guardar_en_mysql(datos_json)

# --- Historico: traduccion de hora local a UTC ---
def a_utc(texto):
    """Convierte '2026-09-15T18:00' (hora de Colombia) a texto UTC para MySQL.

    El navegador envia la hora que el usuario ve en pantalla, sin zona horaria.
    MySQL guarda en UTC. Esta funcion es el traductor entre los dos mundos.
    """
    local = datetime.fromisoformat(texto).replace(tzinfo=BOGOTA)
    return local.astimezone(UTC).strftime('%Y-%m-%d %H:%M:%S')

# --- Historico: asignar cada punto a un viaje ---
def asignar_viajes(filas, viajes):
    """Devuelve, para cada fila, el id del viaje al que pertenece (o None).

    filas:  lista de dicts con 'fecha_registro' (UTC), ordenada ascendente.
    viajes: lista de dicts {'id','inicio','fin'} (UTC), ordenada por inicio.
            'fin' puede ser None si el viaje nunca se cerro.

    Un punto pertenece al ultimo viaje que empezo antes que el, siempre que
    no haya terminado. El "fin efectivo" de un viaje es, en orden:
      1) su fin real,
      2) el inicio del siguiente viaje (si nunca se cerro),
      3) el ultimo punto consultado (si es el ultimo y sigue abierto).
    Los puntos fuera de todo viaje quedan en None (tramo "sin viaje").
    """
    if not viajes:
        return [None] * len(filas)

    ultimo_instante = filas[-1]['fecha_registro'] if filas else None
    efectivos = []
    for i, viaje in enumerate(viajes):
        if viaje['fin'] is not None:
            efectivos.append(viaje['fin'])
        elif i + 1 < len(viajes):
            efectivos.append(viajes[i + 1]['inicio'])
        else:
            efectivos.append(ultimo_instante)

    ids = []
    j = -1
    for fila in filas:
        t = fila['fecha_registro']
        while j + 1 < len(viajes) and viajes[j + 1]['inicio'] <= t:
            j += 1
        if j >= 0 and (efectivos[j] is None or t <= efectivos[j] + TOLERANCIA_FIN_VIAJE):
            ids.append(viajes[j]['id'])
        else:
            ids.append(None)
    return ids

def formatear_marca(marca_utc):
    """datetime UTC sin zona -> texto en hora de Colombia."""
    marca = marca_utc.replace(tzinfo=UTC).astimezone(BOGOTA)
    return marca.strftime('%d/%m/%Y %I:%M:%S %p')

# --- Historico: consulta de la ventana de tiempo ---
def consultar_historial(desde_utc, hasta_utc, vehiculos=VEHICULOS):
    """Devuelve (puntos, viajes, por_vehiculo) entre dos instantes.

    Cada vehiculo se consulta por separado, con su propio tope de
    LIMITE_PUNTOS, para que uno con muchos datos no deje sin ruta al otro.
    Los puntos de cada vehiculo van en orden cronologico y llevan 'vehiculo'
    y 'viaje_id' (None si no cae dentro de ningun viaje). Los viajes tambien
    llevan 'vehiculo'. por_vehiculo = {1: {'total', 'truncado'}, ...}.

    Se filtra por fecha_registro (timestamp) y no por fecha/hora_col, porque
    esas dos son varchar y no se pueden comparar ni ordenar correctamente.
    """
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    usa_col_ubic = tabla_tiene_vehiculo('ubicaciones')
    usa_col_viajes = tabla_tiene_vehiculo('viajes')

    puntos = []
    viajes = []
    por_vehiculo = {}

    for v in vehiculos:
        # Sin la columna (instancia sin migrar) solo existe el vehiculo 1.
        if not usa_col_ubic and v != VEHICULO_POR_DEFECTO:
            por_vehiculo[v] = {'total': 0, 'truncado': False}
            continue

        if usa_col_ubic:
            cursor.execute(
                """
                SELECT latitud, longitud, fecha_registro
                FROM ubicaciones
                WHERE vehiculo = %s AND fecha_registro BETWEEN %s AND %s
                ORDER BY fecha_registro ASC
                LIMIT %s
                """,
                (v, desde_utc, hasta_utc, LIMITE_PUNTOS)
            )
        else:
            cursor.execute(
                """
                SELECT latitud, longitud, fecha_registro
                FROM ubicaciones
                WHERE fecha_registro BETWEEN %s AND %s
                ORDER BY fecha_registro ASC
                LIMIT %s
                """,
                (desde_utc, hasta_utc, LIMITE_PUNTOS)
            )
        filas_crudas = cursor.fetchall()
        truncado = len(filas_crudas) >= LIMITE_PUNTOS

        # Se descartan filas cuyo texto no es una coordenada (basura que haya
        # entrado a la tabla): antes una sola fila asi rompia toda la consulta.
        filas = []
        for fila in filas_crudas:
            try:
                fila['lat'] = float(fila['latitud'])
                fila['lon'] = float(fila['longitud'])
            except (TypeError, ValueError):
                continue
            filas.append(fila)

        # Viajes de ese vehiculo que se solapan con la ventana. Si la tabla no
        # existe o falla, el historial sigue funcionando (un solo recorrido).
        viajes_db = []
        try:
            if usa_col_viajes:
                cursor.execute(
                    """
                    SELECT id, inicio, fin
                    FROM viajes
                    WHERE vehiculo = %s AND inicio <= %s AND (fin IS NULL OR fin >= %s)
                    ORDER BY inicio ASC
                    """,
                    (v, hasta_utc, desde_utc)
                )
                viajes_db = cursor.fetchall()
            elif v == VEHICULO_POR_DEFECTO:
                cursor.execute(
                    """
                    SELECT id, inicio, fin
                    FROM viajes
                    WHERE inicio <= %s AND (fin IS NULL OR fin >= %s)
                    ORDER BY inicio ASC
                    """,
                    (hasta_utc, desde_utc)
                )
                viajes_db = cursor.fetchall()
        except Exception as error:
            print(f"Aviso: no se pudo leer la tabla viajes: {error}")
            viajes_db = []

        ids = asignar_viajes(filas, viajes_db)

        for fila, viaje_id in zip(filas, ids):
            # fecha_registro sale de MySQL en UTC y sin zona: se la marcamos
            # y la traducimos a hora de Colombia para mostrarla.
            puntos.append({
                'lat': fila['lat'],
                'lon': fila['lon'],
                'hora': formatear_marca(fila['fecha_registro']),
                # Instante real (ms desde 1970, UTC): permite ordenar en el
                # navegador los puntos de los dos vehiculos en una sola linea de tiempo.
                't': int(fila['fecha_registro'].replace(tzinfo=UTC).timestamp() * 1000),
                'viaje_id': viaje_id,
                'vehiculo': v
            })

        # Solo se informan los viajes que realmente tienen puntos en la consulta.
        usados = set(ids)
        for viaje in viajes_db:
            if viaje['id'] in usados:
                viajes.append({
                    'id': viaje['id'],
                    'vehiculo': v,
                    'inicio': formatear_marca(viaje['inicio']),
                    'fin': formatear_marca(viaje['fin']) if viaje['fin'] else None,
                    'sin_cierre': viaje['fin'] is None
                })
        por_vehiculo[v] = {'total': len(filas), 'truncado': truncado}

    cursor.close()
    conexion.close()
    return puntos, viajes, por_vehiculo

def leer_vehiculos(texto):
    """'todos' (o vacio) -> (1, 2); '1' o '2' -> ese vehiculo; otro -> None."""
    if texto is None or texto == '' or texto == 'todos':
        return VEHICULOS
    if texto.isdigit() and int(texto) in VEHICULOS:
        return (int(texto),)
    return None

# --- Lugar: distancia entre dos coordenadas (formula de Haversine) ---
def distancia_metros(lat1, lon1, lat2, lon2):
    """Distancia en metros entre dos puntos sobre la superficie de la Tierra,
    tratada como una esfera de radio 6,371,000 metros.

    No sirve restar latitudes/longitudes directamente: un grado de longitud
    representa menos distancia real a medida que uno se acerca a los polos.
    Es la misma formula que ya se uso en JavaScript para la distancia total
    del recorrido, aqui aplicada como filtro en vez de como suma.
    """
    radio_tierra = 6371000
    rad_lat1, rad_lat2 = math.radians(lat1), math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(rad_lat1) * math.cos(rad_lat2) * math.sin(delta_lon / 2) ** 2)
    return 2 * radio_tierra * math.asin(math.sqrt(a))

# --- Lugar: busca el viaje (tabla viajes) que contiene una visita ---
def buscar_viaje_de_visita(cursor, entrada_utc, salida_utc, vehiculo):
    """Devuelve (inicio, fin) del viaje de ese vehiculo que abarca la visita, o None."""
    try:
        if tabla_tiene_vehiculo('viajes'):
            cursor.execute("""
                SELECT inicio, fin FROM viajes
                WHERE vehiculo = %s AND inicio <= %s AND fin >= %s
                ORDER BY inicio DESC
                LIMIT 1
            """, (vehiculo, entrada_utc, salida_utc))
        elif vehiculo == VEHICULO_POR_DEFECTO:
            cursor.execute("""
                SELECT inicio, fin FROM viajes
                WHERE inicio <= %s AND fin >= %s
                ORDER BY inicio DESC
                LIMIT 1
            """, (entrada_utc, salida_utc))
        else:
            return None
        return cursor.fetchone()
    except Exception as e:
        print(f"Aviso: error al buscar viaje de la visita: {e}")
        return None

# --- Lugar: agrupa los puntos cercanos en "visitas" ---
def encontrar_visitas(lat_lugar, lon_lugar, desde_utc=None, hasta_utc=None, vehiculos=VEHICULOS):
    """Recorre el historial de ubicaciones en orden cronologico y agrupa
    los puntos consecutivos que caen dentro de RADIO_LUGAR_METROS en una
    sola "visita" (hora de entrada, hora de salida), en vez de devolver cada
    punto suelto. Por cada visita, busca ademas el viaje completo (tabla
    viajes) que la contiene, para que el frontend pueda dibujar la ruta
    entera del viaje al hacer clic, no solo el tramo cercano al lugar.

    Cada vehiculo se recorre por separado (si se mezclaran sus puntos, las
    visitas se cortarian cada vez que el otro vehiculo estuviera lejos) y
    cada visita lleva 'vehiculo'.

    desde_utc y hasta_utc son opcionales (texto UTC, como los devuelve a_utc).
    Si vienen, solo se consideran los puntos guardados dentro de ese rango;
    si no vienen, se recorre todo el historial. Una visita que empezo antes
    del rango o terminaba despues aparece recortada a los puntos del rango.
    """
    usa_col = tabla_tiene_vehiculo('ubicaciones')
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    visitas = []

    for v in vehiculos:
        if not usa_col and v != VEHICULO_POR_DEFECTO:
            continue

        # Las condiciones se arman solo con fragmentos fijos de SQL; los
        # valores viajan aparte como parametros (%s), nunca pegados al texto.
        condiciones = []
        parametros = []
        if usa_col:
            condiciones.append("vehiculo = %s")
            parametros.append(v)
        if desde_utc:
            condiciones.append("fecha_registro >= %s")
            parametros.append(desde_utc)
        if hasta_utc:
            condiciones.append("fecha_registro <= %s")
            parametros.append(hasta_utc)

        consulta = "SELECT latitud, longitud, fecha_registro FROM ubicaciones"
        if condiciones:
            consulta += " WHERE " + " AND ".join(condiciones)
        consulta += " ORDER BY fecha_registro ASC"

        cursor.execute(consulta, parametros)
        filas = cursor.fetchall()

        # Primera pasada: agrupar en visitas crudas (con datetime en UTC, tal
        # como vienen de MySQL, para poder consultar despues la tabla viajes
        # con los mismos valores).
        visitas_crudas = []
        visita_actual = None

        for fila in filas:
            try:
                lat = float(fila['latitud'])
                lon = float(fila['longitud'])
            except (TypeError, ValueError):
                continue

            distancia = distancia_metros(lat_lugar, lon_lugar, lat, lon)
            marca_utc = fila['fecha_registro']

            if distancia <= RADIO_LUGAR_METROS:
                if visita_actual is None:
                    visita_actual = {'entrada_utc': marca_utc, 'salida_utc': marca_utc, 'puntos': 1}
                else:
                    visita_actual['salida_utc'] = marca_utc
                    visita_actual['puntos'] += 1
            else:
                if visita_actual is not None:
                    visitas_crudas.append(visita_actual)
                    visita_actual = None

        # Si el ultimo punto del historial seguia dentro del radio, esa visita
        # nunca se cerro dentro del bucle: se agrega aqui al terminar.
        if visita_actual is not None:
            visitas_crudas.append(visita_actual)

        # Segunda pasada: convertir cada visita cruda al formato final que
        # espera el frontend, y buscarle su viaje completo si existe.
        for cruda in visitas_crudas:
            entrada_utc = cruda['entrada_utc']
            salida_utc = cruda['salida_utc']

            entrada_local = entrada_utc.replace(tzinfo=UTC).astimezone(BOGOTA)
            salida_local = salida_utc.replace(tzinfo=UTC).astimezone(BOGOTA)

            viaje = buscar_viaje_de_visita(cursor, entrada_utc, salida_utc, v)
            if viaje:
                viaje_inicio_local = viaje['inicio'].replace(tzinfo=UTC).astimezone(BOGOTA)
                viaje_fin_local = viaje['fin'].replace(tzinfo=UTC).astimezone(BOGOTA)
                viaje_inicio_iso = viaje_inicio_local.strftime('%Y-%m-%dT%H:%M:%S')
                viaje_fin_iso = viaje_fin_local.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                viaje_inicio_iso = None
                viaje_fin_iso = None

            visitas.append({
                'vehiculo': v,
                'entrada': entrada_local.strftime('%d/%m/%Y %I:%M:%S %p'),
                'salida': salida_local.strftime('%d/%m/%Y %I:%M:%S %p'),
                'entrada_iso': entrada_local.strftime('%Y-%m-%dT%H:%M:%S'),
                'salida_iso': salida_local.strftime('%Y-%m-%dT%H:%M:%S'),
                'puntos': cruda['puntos'],
                'viaje_inicio': viaje_inicio_iso,
                'viaje_fin': viaje_fin_iso
            })
    cursor.close()
    conexion.close()
    return visitas

@app.route('/')
def index():
    return render_template('index.html', nombre_pagina=NOMBRE_PAGINA)

# --- Historico: pagina y endpoint de consulta ---
@app.route('/historial')
def historial():
    return render_template('historial.html', nombre_pagina=NOMBRE_PAGINA)

@app.route('/api/historial')
def api_historial():
    desde = request.args.get('desde')
    hasta = request.args.get('hasta')

    # Solo "desde" es obligatorio. Sin "hasta" se consulta hasta este momento.
    if not desde:
        return jsonify({'error': 'Falta el parametro desde'}), 400

    try:
        desde_utc = a_utc(desde)
        if hasta:
            hasta_utc = a_utc(hasta)
        else:
            hasta_utc = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return jsonify({'error': 'Formato de fecha invalido'}), 400

    if desde_utc >= hasta_utc:
        return jsonify({'error': 'La fecha inicial debe ser anterior a la final'}), 400

    # vehiculo=todos (por defecto), 1 o 2
    vehiculos = leer_vehiculos(request.args.get('vehiculo'))
    if vehiculos is None:
        return jsonify({'error': 'El parametro vehiculo debe ser todos, 1 o 2'}), 400

    try:
        puntos, viajes, por_vehiculo = consultar_historial(desde_utc, hasta_utc, vehiculos)
    except Exception as error:
        print(f"Error consultando el historial: {error}")
        return jsonify({'error': 'No se pudo consultar la base de datos'}), 500

    return jsonify({
        'puntos': puntos,
        'viajes': viajes,
        'total': len(puntos),
        'truncado': any(info['truncado'] for info in por_vehiculo.values()),
        'por_vehiculo': {str(v): info for v, info in por_vehiculo.items()}
    })

# --- Lugar: endpoint de consulta ---
@app.route('/api/lugar')
def api_lugar():
    lat_texto = request.args.get('lat')
    lon_texto = request.args.get('lon')

    if not lat_texto or not lon_texto:
        return jsonify({'error': 'Faltan los parametros lat y lon'}), 400

    try:
        lat_lugar = float(lat_texto)
        lon_lugar = float(lon_texto)
    except ValueError:
        return jsonify({'error': 'lat y lon deben ser numeros validos'}), 400

    # Primero fecha, despues lugar: "desde" es OBLIGATORIO y "hasta" opcional
    # (sin "hasta" se busca hasta ahora). Llegan en hora de Colombia, igual
    # que en /api/historial.
    desde = request.args.get('desde')
    hasta = request.args.get('hasta')

    if not desde:
        return jsonify({'error': 'Primero elige la fecha inicial: falta el parametro desde'}), 400

    try:
        desde_utc = a_utc(desde)
        hasta_utc = a_utc(hasta) if hasta else None
    except ValueError:
        return jsonify({'error': 'Formato de fecha invalido'}), 400

    if hasta_utc and desde_utc >= hasta_utc:
        return jsonify({'error': 'La fecha inicial debe ser anterior a la final'}), 400

    vehiculos = leer_vehiculos(request.args.get('vehiculo'))
    if vehiculos is None:
        return jsonify({'error': 'El parametro vehiculo debe ser todos, 1 o 2'}), 400

    try:
        visitas = encontrar_visitas(lat_lugar, lon_lugar, desde_utc, hasta_utc, vehiculos)
    except Exception as error:
        print(f"Error consultando el lugar: {error}")
        return jsonify({'error': 'No se pudo consultar la base de datos'}), 500

    return jsonify({
        'radio_metros': RADIO_LUGAR_METROS,
        'visitas': visitas,
        'total_visitas': len(visitas)
    })

# --- Direccion y barrio del vehiculo (geocodificacion inversa) ---
# Nominatim (OpenStreetMap) permite ~1 consulta por segundo y exige un
# User-Agent propio. Por eso se consulta desde el servidor, con cache por
# celda de ~25 m y un minimo de 1.1 s entre consultas reales.
_cache_direccion = {}
_ultima_consulta_nominatim = 0.0
_candado_nominatim = threading.Lock()

@app.route('/api/direccion')
def api_direccion():
    global _ultima_consulta_nominatim
    try:
        lat = float(request.args.get('lat', ''))
        lon = float(request.args.get('lon', ''))
    except ValueError:
        return jsonify({'error': 'lat y lon deben ser numeros validos'}), 400

    clave = (round(lat, 4), round(lon, 4))
    if clave in _cache_direccion:
        return jsonify(_cache_direccion[clave])

    with _candado_nominatim:
        espera = 1.1 - (time.time() - _ultima_consulta_nominatim)
        if espera > 0:
            time.sleep(espera)
        _ultima_consulta_nominatim = time.time()
        url = 'https://nominatim.openstreetmap.org/reverse?' + urllib.parse.urlencode({
            'format': 'jsonv2', 'lat': lat, 'lon': lon,
            'zoom': 18, 'addressdetails': 1, 'accept-language': 'es'
        })
        peticion = urllib.request.Request(url, headers={'User-Agent': f'TelemetriaGPS/{NOMBRE_PAGINA}'})
        try:
            with urllib.request.urlopen(peticion, timeout=6) as r:
                datos = json.load(r)
        except Exception as error:
            print(f"Error en geocodificacion inversa: {error}")
            return jsonify({'error': 'No se pudo obtener la direccion'}), 502

    d = datos.get('address', {})
    calle = d.get('road') or d.get('pedestrian') or d.get('footway') or ''
    numero = d.get('house_number', '')
    barrio = (d.get('neighbourhood') or d.get('suburb') or d.get('quarter')
              or d.get('city_district') or '')
    ciudad = d.get('city') or d.get('town') or d.get('village') or d.get('municipality') or ''
    resultado = {
        'direccion': (f"{calle} {numero}".strip()) or datos.get('name') or '',
        'barrio': barrio,
        'ciudad': ciudad,
        'completa': datos.get('display_name', '')
    }
    if len(_cache_direccion) > 2000:
        _cache_direccion.clear()
    _cache_direccion[clave] = resultado
    return jsonify(resultado)

if __name__ == '__main__':
    print(f"Página: {NOMBRE_PAGINA}")
    socketio.start_background_task(udp_listener)
    print(f"Iniciando servidor Web en el puerto {WEB_PORT}...")
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, allow_unsafe_werkzeug=True)