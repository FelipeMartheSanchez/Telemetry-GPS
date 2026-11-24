import os
import re
import hmac
import math
import socket
import json
import time
import threading
import urllib.parse
import urllib.request
import mysql.connector
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from flask import Flask, render_template, request, jsonify, redirect
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

# --- Historico: zonas horarias y grupos de viajes por consulta ---
# MySQL guarda en UTC; el usuario piensa en hora de Colombia (UTC-5).
BOGOTA = ZoneInfo('America/Bogota')
UTC = ZoneInfo('UTC')
# Sin tope de puntos: el rango se divide en grupos de VIAJES_POR_GRUPO viajes
# (el 1 es el mas reciente) y cada consulta trae solo los envios de un grupo.
VIAJES_POR_GRUPO = 10
# Margen despues de FIN_RECORRIDO para aceptar el ultimo punto del viaje
# (el telefono puede enviar un punto justo despues de la marca de fin).
TOLERANCIA_FIN_VIAJE = timedelta(seconds=15)

# --- Vehiculos: la app de cada celular manda "Vehiculo: N" en el paquete ---
# Un paquete sin esa linea (app vieja) se asigna al vehiculo 1.
VEHICULOS = (1, 2)
VEHICULO_POR_DEFECTO = 1

# --- Resultados de ML que puede enviar un vehiculo (hoy, el Auto 1 / ESP32) ---
# Cada ubicacion puede traer dos lineas mas: "Estilo: ..." (modelo OBD) y
# "Evento: ..." (modelo IMU). Un vehiculo que no las envia (el celular) las deja
# en NULL: nada se descarta ni se rompe. La lista cerrada impide que entre texto raro.
ESTILOS = ('cauteloso', 'normal', 'agresivo', 'sin_dato')
EVENTOS = ('aceleracion_brusca', 'frenado_brusco', 'giro_derecha_brusco',
           'giro_izquierda_brusco', 'ninguno')
# "Eventos: a,b" trae TODAS las maniobras del tramo de 10 s ("Evento" trae solo la ultima).
MANIOBRAS = ('aceleracion_brusca', 'frenado_brusco', 'giro_derecha_brusco', 'giro_izquierda_brusco')
MAX_EVENTOS_TRAMO = 4
# Columnas que agrega la migracion SQL de ML (MIGRACION_ML_AUTO1.md)
COLUMNAS_SCORE_VIAJE = ('driving_score', 'score_estilo', 'score_eventos',
                        'pct_cauteloso', 'pct_normal', 'pct_agresivo',
                        'n_aceleraciones', 'n_frenados', 'n_giros', 'distancia_km')

# --- Driving score del viaje (se calcula al cerrar el viaje) ---
# El ESP32 envia en cada ubicacion "Calificacion: <0-100>" (o "sin_dato"): la
# calificacion de esos 10 s que da su red neuronal. El score del viaje es el
# PROMEDIO de esas calificaciones (las "sin_dato" no cuentan). No hay formula.
ESPERA_SCORE_S = 20             # > TOLERANCIA_FIN_VIAJE: deja llegar el ultimo punto
# Rangos de la pagina: >= 80 cauteloso, 60-80 normal, < 60 agresivo.
# Un viaje mas corto que SCORE_KM_MINIMOS SI recibe score (se calcula y se guarda),
# pero la pagina no lo muestra. Asi, si mas adelante cambia la regla, ya esta calculado.
SCORE_KM_MINIMOS = 2.0

# --- Cierre automatico de viajes sin datos ---
# Si un vehiculo deja de enviar (la ESP32 sin energia/senal o la app cerrada) por
# mas de TIMEOUT_SIN_DATOS_S, su viaje abierto se cierra solo, con la hora del
# ultimo punto recibido, y se calcula el score. Un viaje abierto que ya quedo
# atras (el vehiculo inicio otro viaje despues) se cierra de inmediato.
TIMEOUT_SIN_DATOS_S = 180
REVISION_INACTIVIDAD_S = 30

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

# --- ¿Existe la columna? (migracion de ML) ---
# Igual que con "vehiculo": una instancia sin migrar sigue funcionando y solo
# deja de guardar / mostrar los campos nuevos. Una vez detectada se recuerda; si
# no existe, se revisa de nuevo cada 30 s.
_cache_columnas = {}
_TABLAS_CON_COLUMNAS = ('ubicaciones', 'viajes')
_COLUMNAS_CONOCIDAS = ('estilo', 'evento', 'calificacion', 'eventos') + COLUMNAS_SCORE_VIAJE

def tabla_tiene_columna(tabla, columna):
    assert tabla in _TABLAS_CON_COLUMNAS and columna in _COLUMNAS_CONOCIDAS
    ahora = time.time()
    previo = _cache_columnas.get((tabla, columna))
    if previo and (previo[0] or ahora - previo[1] < 30):
        return previo[0]
    tiene = False
    try:
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {tabla} LIKE %s", (columna,))
        tiene = cursor.fetchone() is not None
        cursor.close()
        conexion.close()
    except Exception as error:
        print(f"Aviso: no se pudo revisar la columna {columna} de {tabla}: {error}")
    _cache_columnas[(tabla, columna)] = (tiene, ahora)
    return tiene

def tiene_columnas_ml_ubicaciones():
    return tabla_tiene_columna('ubicaciones', 'estilo') and tabla_tiene_columna('ubicaciones', 'evento')

def tiene_columna_calificacion():
    return tabla_tiene_columna('ubicaciones', 'calificacion')

def tiene_columna_eventos():
    return tabla_tiene_columna('ubicaciones', 'eventos')

def tiene_columnas_score_viajes():
    return all(tabla_tiene_columna('viajes', c) for c in COLUMNAS_SCORE_VIAJE)

def extraer_campo(lineas, nombre, validos):
    """Lee "Nombre: valor". Devuelve el valor si esta en la lista; si no viene
    (celular) o no es valido, devuelve None. Nunca descarta el paquete."""
    for linea in lineas:
        m = re.match(rf'^\s*{nombre}\s*:\s*(\w+)\s*$', linea, re.IGNORECASE)
        if m:
            valor = m.group(1).lower()
            return valor if valor in validos else None
    return None

def extraer_calificacion(lineas):
    """Lee "Calificacion: 72.4" (0-100). Devuelve float, o None si no viene,
    viene "sin_dato" o no es un numero valido. Nunca descarta el paquete."""
    for linea in lineas:
        m = re.match(r'^\s*Calificacion\s*:\s*(\S+)\s*$', linea, re.IGNORECASE)
        if m:
            try:
                valor = float(m.group(1))
            except ValueError:
                return None
            return valor if 0 <= valor <= 100 else None
    return None

def extraer_eventos(lineas):
    """Lee "Eventos: frenado_brusco,giro_izquierda_brusco" (o "Eventos: ninguno").
    Devuelve el texto normalizado ("a,b" o "ninguno"), o None si no viene o algun
    nombre no es valido. Nunca descarta el paquete."""
    for linea in lineas:
        m = re.match(r'^\s*Eventos\s*:\s*(.+?)\s*$', linea, re.IGNORECASE)
        if m:
            lista = [e.strip().lower() for e in m.group(1).split(',')]
            if lista == ['ninguno']:
                return 'ninguno'
            if 0 < len(lista) <= MAX_EVENTOS_TRAMO and all(e in MANIOBRAS for e in lista):
                return ','.join(lista)
            return None
    return None

def lista_eventos(fila):
    """Maniobras de un punto: de 'eventos' si existe; si no (puntos viejos o
    firmware sin la linea Eventos), de 'evento'."""
    eventos = fila.get('eventos')
    if eventos:
        return [] if eventos == 'ninguno' else eventos.split(',')
    evento = fila.get('evento')
    return [evento] if evento in MANIOBRAS else []

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
        columnas = ['latitud', 'longitud', 'fecha', 'hora_col']
        valores = [datos['latitud'], datos['longitud'], datos['fecha'], datos['hora_col']]
        if tabla_tiene_vehiculo('ubicaciones'):
            columnas.append('vehiculo')
            valores.append(vehiculo)
        elif vehiculo != VEHICULO_POR_DEFECTO:
            print(f"Aviso: falta la columna vehiculo en ubicaciones; no se guarda el vehiculo {vehiculo}.")
            return
        # Estilo y evento (ML) solo si ya existen las dos columnas. Para un
        # vehiculo que no los envia (el celular) se guardan como NULL.
        if tiene_columnas_ml_ubicaciones():
            columnas += ['estilo', 'evento']
            valores += [datos.get('estilo'), datos.get('evento')]
        if tiene_columna_calificacion():
            columnas.append('calificacion')
            valores.append(datos.get('calificacion'))
        if tiene_columna_eventos():
            columnas.append('eventos')
            valores.append(datos.get('eventos'))
        consulta = (f"INSERT INTO ubicaciones ({', '.join(columnas)}) "
                    f"VALUES ({', '.join(['%s'] * len(columnas))})")
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(consulta, tuple(valores))
        conexion.commit()
        cursor.close()
        conexion.close()
        print(f"--> Registro del vehiculo {vehiculo} guardado en MySQL (AWS RDS) exitosamente.")
    except Exception as error:
        print(f"Error al conectar/guardar en MySQL: {error}")

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
    """Cierra el viaje abierto mas reciente (fin IS NULL) de ese vehiculo y
    programa el calculo de su driving score."""
    try:
        if tabla_tiene_vehiculo('viajes'):
            filtro, valores = "fin IS NULL AND vehiculo = %s", (vehiculo,)
        elif vehiculo == VEHICULO_POR_DEFECTO:
            filtro, valores = "fin IS NULL", ()
        else:
            return
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute(f"SELECT id FROM viajes WHERE {filtro} ORDER BY inicio DESC LIMIT 1", valores)
        fila = cursor.fetchone()
        viaje_id = fila[0] if fila else None
        filas_afectadas = 0
        if viaje_id is not None:
            cursor.execute("UPDATE viajes SET fin = NOW() WHERE id = %s", (viaje_id,))
            conexion.commit()
            filas_afectadas = cursor.rowcount
        cursor.close()
        conexion.close()
        if filas_afectadas == 0:
            print(f"--> FIN_RECORRIDO del vehiculo {vehiculo} recibido, pero no habia ningun viaje abierto.")
        else:
            print(f"--> Viaje del vehiculo {vehiculo} finalizado.")
            # Se espera ESPERA_SCORE_S porque el ultimo punto puede llegar despues
            # de la marca de fin. El Timer no bloquea el receptor UDP.
            temporizador = threading.Timer(ESPERA_SCORE_S, calcular_score_viaje, args=(vehiculo, viaje_id))
            temporizador.daemon = True
            temporizador.start()
    except Exception as error:
        print(f"Error al registrar fin de viaje: {error}")

def a_datetime(valor):
    if isinstance(valor, str):
        return datetime.fromisoformat(valor)
    return valor

def cerrar_viajes_inactivos():
    """Cierra los viajes abiertos (fin IS NULL) que ya no reciben datos.
    Devuelve la lista de (vehiculo, viaje_id) cerrados por falta de datos."""
    cerrados = []
    try:
        if not tabla_tiene_vehiculo('viajes') or not tabla_tiene_vehiculo('ubicaciones'):
            return cerrados
        conexion = conectar_mysql()
        cursor = conexion.cursor(dictionary=True)
        cursor.execute("SELECT NOW() AS ahora")
        ahora = a_datetime(cursor.fetchone()['ahora'])
        cursor.execute("SELECT id, inicio, vehiculo FROM viajes WHERE fin IS NULL ORDER BY vehiculo, inicio")
        abiertos = cursor.fetchall()
        for v in abiertos:
            inicio = a_datetime(v['inicio'])
            cursor.execute("SELECT MIN(inicio) AS siguiente FROM viajes WHERE vehiculo = %s AND inicio > %s",
                           (v['vehiculo'], inicio))
            siguiente = cursor.fetchone()['siguiente']
            if siguiente is not None:
                siguiente = a_datetime(siguiente)
                cursor.execute("SELECT MAX(fecha_registro) AS ultimo FROM ubicaciones "
                               "WHERE vehiculo = %s AND fecha_registro >= %s AND fecha_registro < %s",
                               (v['vehiculo'], inicio, siguiente))
            else:
                cursor.execute("SELECT MAX(fecha_registro) AS ultimo FROM ubicaciones "
                               "WHERE vehiculo = %s AND fecha_registro >= %s", (v['vehiculo'], inicio))
            ultimo = cursor.fetchone()['ultimo']
            ultimo = max(inicio, a_datetime(ultimo)) if ultimo is not None else inicio
            if siguiente is None and (ahora - ultimo).total_seconds() < TIMEOUT_SIN_DATOS_S:
                continue                       # sigue activo
            cursor.execute("UPDATE viajes SET fin = %s WHERE id = %s AND fin IS NULL", (ultimo, v['id']))
            conexion.commit()
            if cursor.rowcount:
                print(f"--> Viaje {v['id']} del vehiculo {v['vehiculo']} cerrado por falta de datos "
                      f"(ultimo punto: {ultimo}).")
                cerrados.append((v['vehiculo'], v['id'], siguiente is None))
        cursor.close()
        conexion.close()
    except Exception as error:
        print(f"Error al revisar viajes inactivos: {error}")
        return []
    resultado = []
    for vehiculo, viaje_id, activo_hasta_ahora in cerrados:
        if activo_hasta_ahora:
            socketio.emit('viaje_finalizado', {'vehiculo': vehiculo, 'motivo': 'sin_datos'})
        calcular_score_viaje(vehiculo, viaje_id)
        resultado.append((vehiculo, viaje_id))
    return resultado

def vigilar_inactividad():
    while True:
        cerrar_viajes_inactivos()
        time.sleep(REVISION_INACTIVIDAD_S)

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

# --- Calificacion del viaje en vivo ---
# Por vehiculo: suma y cantidad de calificaciones de 10 s del viaje actual y los km
# recorridos. Se reinicia con INICIO_RECORRIDO. Si el servidor se reinicia a mitad de
# un viaje, se reconstruye desde la base de datos con el primer punto que llegue.
_viaje_vivo = {}
_lock_viaje_vivo = threading.Lock()

def reiniciar_viaje_vivo(vehiculo):
    with _lock_viaje_vivo:
        _viaje_vivo[vehiculo] = {'suma': 0.0, 'n': 0, 'metros': 0.0, 'previo': None, 'ultima_hora': None}

def reconstruir_viaje_vivo(vehiculo):
    """Estado del viaje abierto de ese vehiculo leido de la base de datos (o vacio)."""
    estado = {'suma': 0.0, 'n': 0, 'metros': 0.0, 'previo': None, 'ultima_hora': None}
    try:
        if not tabla_tiene_vehiculo('viajes') or not tabla_tiene_vehiculo('ubicaciones'):
            return estado
        conexion = conectar_mysql()
        cursor = conexion.cursor(dictionary=True)
        cursor.execute("SELECT inicio FROM viajes WHERE vehiculo = %s AND fin IS NULL ORDER BY inicio DESC LIMIT 1",
                       (vehiculo,))
        viaje = cursor.fetchone()
        if viaje:
            extra = ", calificacion" if tiene_columna_calificacion() else ""
            cursor.execute(f"SELECT latitud, longitud, fecha, hora_col{extra} FROM ubicaciones "
                           "WHERE vehiculo = %s AND fecha_registro >= %s ORDER BY fecha_registro ASC",
                           (vehiculo, viaje['inicio']))
            for f in cursor.fetchall():
                sumar_punto_vivo(estado, f.get('latitud'), f.get('longitud'), f.get('calificacion'),
                                 (f.get('fecha'), f.get('hora_col')))
        cursor.close()
        conexion.close()
    except Exception as error:
        print(f"Aviso: no se pudo reconstruir el viaje en vivo del vehiculo {vehiculo}: {error}")
    return estado

def sumar_punto_vivo(estado, lat_texto, lon_texto, calificacion, hora):
    if hora == estado['ultima_hora']:
        return                                  # mensaje repetido: no se cuenta dos veces
    estado['ultima_hora'] = hora
    try:
        punto = (float(lat_texto), float(lon_texto))
        if estado['previo']:
            estado['metros'] += distancia_metros(*estado['previo'], *punto)
        estado['previo'] = punto
    except (TypeError, ValueError):
        pass
    if calificacion is not None:
        estado['suma'] += float(calificacion)
        estado['n'] += 1

def actualizar_viaje_vivo(datos):
    """Suma el punto al viaje en vivo. Devuelve calificacion_viaje (promedio hasta
    ahora, o None) y km_viaje. Para un vehiculo que no envia calificacion, ambos None."""
    if datos.get('estilo') is None and datos.get('calificacion') is None:
        return {'calificacion_viaje': None, 'km_viaje': None}      # celular: sin ML
    vehiculo = datos['vehiculo']
    with _lock_viaje_vivo:
        estado = _viaje_vivo.get(vehiculo)
    if estado is None:
        estado = reconstruir_viaje_vivo(vehiculo)
    with _lock_viaje_vivo:
        estado = _viaje_vivo.setdefault(vehiculo, estado)
        sumar_punto_vivo(estado, datos['latitud'], datos['longitud'], datos.get('calificacion'),
                         (datos['fecha'], datos['hora_col']))
        promedio = round(estado['suma'] / estado['n'], 1) if estado['n'] else None
        return {'calificacion_viaje': promedio, 'km_viaje': round(estado['metros'] / 1000, 2)}

# --- Origen de la ubicacion del Auto 1: ESP32 o emulador ---
# UBICACION_DESDE en el .env (si no esta: esp32, el comportamiento de siempre).
#   esp32    -> la ESP32 envia ubicacion + estilo/eventos, y abre y cierra el viaje.
#   emulador -> la RUTA del Auto 1 viene del emulador (/emulador, mensajes con la
#               linea "Origen: emulador"), que tambien abre y cierra el viaje. De la
#               ESP32 solo se usan Estilo, Calificacion y las maniobras: su ubicacion
#               y sus marcas INICIO/FIN se descartan. Cada ubicacion del emulador se
#               guarda con el ultimo estilo/calificacion de la ESP32 (si tiene menos
#               de ML_ESP32_MAX_EDAD_S) y con las maniobras recibidas desde el punto
#               anterior. El Auto 2 (celular) no cambia.
UBICACION_DESDE = os.getenv('UBICACION_DESDE', 'esp32').strip().lower()
if UBICACION_DESDE not in ('esp32', 'emulador'):
    print(f"Aviso: UBICACION_DESDE={UBICACION_DESDE!r} no es valido (esp32 | emulador); se usa esp32.")
    UBICACION_DESDE = 'esp32'
VEHICULO_EMULADO = 1            # el Auto 1 (ESP32), el mismo que imita el emulador
ML_ESP32_MAX_EDAD_S = 30        # estilo/calificacion/maniobras mas viejos no se usan
_aviso_datos_ml_t = 0.0         # ultima vez que se aviso que se ignoran "Datos ML" (modo esp32)

# Ultimos resultados de la ESP32 (los escribe y lee solo udp_listener; el lock es por seguridad)
_ml_esp32 = {'estilo': None, 'calificacion': None, 't': None, 'maniobras': []}   # maniobras: [(t, nombre)]
_lock_ml_esp32 = threading.Lock()

def es_del_emulador(lineas):
    """True si el paquete trae la linea "Origen: emulador"."""
    return any(re.match(r'^\s*Origen\s*:\s*emulador\s*$', l, re.IGNORECASE) for l in lineas)

def ruta_desde_emulador(vehiculo):
    """True si la ubicacion de este vehiculo debe venir del emulador."""
    return UBICACION_DESDE == 'emulador' and vehiculo == VEHICULO_EMULADO

def guardar_ml_esp32(datos):
    """Guarda el estilo/calificacion de un mensaje de la ESP32 y acumula sus maniobras."""
    ahora = time.monotonic()
    nuevas = lista_eventos(datos)               # de 'eventos' o, si no viene, de 'evento'
    with _lock_ml_esp32:
        _ml_esp32['estilo'] = datos.get('estilo')
        _ml_esp32['calificacion'] = datos.get('calificacion')
        _ml_esp32['t'] = ahora
        corte = ahora - ML_ESP32_MAX_EDAD_S
        _ml_esp32['maniobras'] = [x for x in _ml_esp32['maniobras'] if x[0] >= corte] + [(ahora, m) for m in nuevas]

def limpiar_maniobras_esp32():
    with _lock_ml_esp32:
        _ml_esp32['maniobras'] = []

def aplicar_ml_esp32(datos):
    """Pone en una ubicacion del emulador el estilo/calificacion de la ESP32 (si son
    recientes) y las maniobras acumuladas desde la ubicacion anterior (max. 4, las
    mas recientes). Las maniobras usadas se borran: ninguna se cuenta dos veces."""
    ahora = time.monotonic()
    with _lock_ml_esp32:
        reciente = _ml_esp32['t'] is not None and ahora - _ml_esp32['t'] <= ML_ESP32_MAX_EDAD_S
        corte = ahora - ML_ESP32_MAX_EDAD_S
        maniobras = [m for t, m in _ml_esp32['maniobras'] if t >= corte][-MAX_EVENTOS_TRAMO:]
        _ml_esp32['maniobras'] = []
        estilo = _ml_esp32['estilo'] if reciente else None
        calificacion = _ml_esp32['calificacion'] if reciente else None
    # Sin datos recientes de la ESP32 el punto queda "sin dato" (no se inventa nada).
    datos['estilo'] = estilo or 'sin_dato'
    datos['calificacion'] = calificacion
    datos['evento'] = maniobras[-1] if maniobras else 'ninguno'
    datos['eventos'] = ','.join(maniobras) if maniobras else 'ninguno'

def udp_listener():
    global _aviso_datos_ml_t
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
            if ruta_desde_emulador(vehiculo) and not es_del_emulador(lineas):
                print(f"--> {marca} de la ESP32 ignorado: el viaje del vehiculo {vehiculo} "
                      f"lo controla el emulador (UBICACION_DESDE=emulador).")
                continue
            if marca == "INICIO_RECORRIDO":
                if ruta_desde_emulador(vehiculo):
                    limpiar_maniobras_esp32()      # las de antes del viaje no cuentan
                reiniciar_viaje_vivo(vehiculo)
                registrar_inicio_viaje(vehiculo)
            else:
                registrar_fin_viaje(vehiculo)
            continue

        # "Datos ML": mensaje de la ESP32 en modo sintetico, sin coordenadas (estilo,
        # evento, calificacion y eventos cada 10 s). Solo sirve con
        # UBICACION_DESDE=emulador: se guarda y se pone en la ruta del emulador.
        if marca == "Datos ML":
            vehiculo = extraer_vehiculo(lineas[1:])
            if vehiculo is None:
                print(f"Datos ML descartados: vehiculo invalido desde {addr[0]}.")
                continue
            if not ruta_desde_emulador(vehiculo):
                if time.monotonic() - _aviso_datos_ml_t >= 60:       # un aviso por minuto
                    _aviso_datos_ml_t = time.monotonic()
                    print(f"--> Datos ML del vehiculo {vehiculo} ignorados: solo se usan para el vehiculo "
                          f"{VEHICULO_EMULADO} con UBICACION_DESDE=emulador (ahora: {UBICACION_DESDE}).")
                continue
            guardar_ml_esp32({
                'estilo': extraer_campo(lineas[1:], 'Estilo', ESTILOS),
                'evento': extraer_campo(lineas[1:], 'Evento', EVENTOS),
                'calificacion': extraer_calificacion(lineas[1:]),
                'eventos': extraer_eventos(lineas[1:]),
            })
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
                'vehiculo': vehiculo,
                # Solo el Auto 1 (ESP32) los envia; el celular los deja en None.
                'estilo': extraer_campo(lineas[5:], 'Estilo', ESTILOS),
                'evento': extraer_campo(lineas[5:], 'Evento', EVENTOS),
                'calificacion': extraer_calificacion(lineas[5:]),
                'eventos': extraer_eventos(lineas[5:])
            }
            if ruta_desde_emulador(vehiculo):
                if not es_del_emulador(lineas):
                    # ESP32: su ubicacion se descarta; solo se guarda su estilo/eventos.
                    guardar_ml_esp32(datos_json)
                    continue
                # Emulador: su ruta + el estilo/eventos de la ESP32.
                aplicar_ml_esp32(datos_json)
            # Promedio del viaje hasta ahora y km recorridos (para la tarjeta en vivo)
            datos_json.update(actualizar_viaje_vivo(datos_json))

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

# --- Historico: grupos de viajes ---
def _a_datetime(valor):
    """Texto UTC 'YYYY-MM-DD HH:MM:SS' (o datetime) -> datetime sin zona."""
    return valor if isinstance(valor, datetime) else datetime.strptime(valor, '%Y-%m-%d %H:%M:%S')

def calcular_grupos(cursor, desde_utc, hasta_utc, vehiculos, usa_col_ubic, usa_col_viajes):
    """Divide el rango en grupos de VIAJES_POR_GRUPO viajes, del mas reciente al mas antiguo.

    Devuelve (grupos, numeros):
      grupos:  lista de {'desde', 'hasta', 'incluye_hasta', 'viajes'} (datetimes UTC).
               grupos[0] es el mas reciente. Las ventanas son contiguas y no se
               solapan: cada envio del rango cae en un solo grupo, tambien los que
               estan fuera de todo viaje.
      numeros: id de viaje -> numero del viaje (por vehiculo, en orden cronologico
               en TODO el rango), para que "Viaje 16" sea el mismo en cualquier grupo.
    Los viajes de los dos vehiculos se ordenan juntos por hora de inicio. Solo
    cuentan los viajes con al menos un envio. Sin viajes (o sin la columna
    vehiculo) hay un solo grupo con todo el rango.
    """
    desde = _a_datetime(desde_utc)
    hasta = _a_datetime(hasta_utc)
    filas = []
    if usa_col_ubic and usa_col_viajes:
        try:
            marcas = ', '.join(['%s'] * len(vehiculos))
            cursor.execute(
                f"""
                SELECT v.id, v.vehiculo, v.inicio FROM viajes v
                WHERE v.vehiculo IN ({marcas}) AND v.inicio <= %s AND (v.fin IS NULL OR v.fin >= %s)
                  AND EXISTS (
                      SELECT 1 FROM ubicaciones u
                      WHERE u.vehiculo = v.vehiculo
                        AND u.fecha_registro >= GREATEST(v.inicio, %s)
                        AND u.fecha_registro <= LEAST(COALESCE(DATE_ADD(v.fin, INTERVAL %s SECOND), %s), %s))
                ORDER BY v.inicio DESC, v.id DESC
                """,
                (*vehiculos, hasta, desde, desde,
                 int(TOLERANCIA_FIN_VIAJE.total_seconds()), hasta, hasta)
            )
            filas = cursor.fetchall()
        except Exception as error:
            print(f"Aviso: no se pudieron agrupar los viajes: {error}")
            filas = []

    numeros, conteo = {}, {}
    for f in reversed(filas):                      # del mas antiguo al mas reciente
        conteo[f['vehiculo']] = conteo.get(f['vehiculo'], 0) + 1
        numeros[f['id']] = conteo[f['vehiculo']]

    if not filas:
        return [{'desde': desde, 'hasta': hasta, 'incluye_hasta': True, 'viajes': 0}], numeros

    grupos = []
    for k in range(0, len(filas), VIAJES_POR_GRUPO):
        bloque = filas[k:k + VIAJES_POR_GRUPO]
        # El grupo empieza con su viaje mas antiguo (sin salirse del rango).
        grupos.append({'viajes': len(bloque), 'desde': max(bloque[-1]['inicio'], desde)})
    for i, g in enumerate(grupos):
        # Termina donde empieza el grupo mas reciente; el grupo 1 llega hasta "hasta".
        g['hasta'] = hasta if i == 0 else grupos[i - 1]['desde']
        g['incluye_hasta'] = (i == 0)
    grupos[-1]['desde'] = desde                    # el mas antiguo cubre desde el inicio del rango
    return grupos, numeros

# --- Historico: consulta de la ventana de tiempo ---
def consultar_historial(desde_utc, hasta_utc, vehiculos=VEHICULOS, grupo=1):
    """Devuelve (puntos, viajes, por_vehiculo, grupos) de UN grupo de viajes del rango.

    Sin tope de puntos: el rango se divide en grupos de VIAJES_POR_GRUPO viajes
    (calcular_grupos) y se traen todos los envios de la ventana del grupo pedido
    (1 = el mas reciente). grupos es la lista de todos los grupos, para el selector.
    Cada vehiculo se consulta por separado. Los puntos van en orden cronologico y
    llevan 'vehiculo' y 'viaje_id' (None si no cae dentro de ningun viaje). Los
    viajes llevan 'vehiculo' y 'numero'. por_vehiculo = {1: {'total', 'truncado'}, ...}
    (truncado siempre es False: se conserva por compatibilidad).

    Se filtra por fecha_registro (timestamp) y no por fecha/hora_col, porque
    esas dos son varchar y no se pueden comparar ni ordenar correctamente.
    """
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    usa_col_ubic = tabla_tiene_vehiculo('ubicaciones')
    usa_col_viajes = tabla_tiene_vehiculo('viajes')
    usa_ml = tiene_columnas_ml_ubicaciones()
    usa_score = tiene_columnas_score_viajes()
    extra_ubic = (", estilo, evento" if usa_ml else "") + (", eventos" if tiene_columna_eventos() else "")
    extra_viaje = (", " + ", ".join(COLUMNAS_SCORE_VIAJE)) if usa_score else ""

    puntos = []
    viajes = []
    por_vehiculo = {}

    grupos, numeros = calcular_grupos(cursor, desde_utc, hasta_utc, vehiculos, usa_col_ubic, usa_col_viajes)
    grupo = min(max(grupo, 1), len(grupos))
    ventana = grupos[grupo - 1]
    desde_g, hasta_g = ventana['desde'], ventana['hasta']
    # El limite superior es exclusivo salvo en el grupo 1: asi un envio justo en
    # el borde no aparece en dos grupos.
    op_hasta = '<=' if ventana['incluye_hasta'] else '<'

    for v in vehiculos:
        # Sin la columna (instancia sin migrar) solo existe el vehiculo 1.
        if not usa_col_ubic and v != VEHICULO_POR_DEFECTO:
            por_vehiculo[v] = {'total': 0, 'truncado': False}
            continue

        if usa_col_ubic:
            cursor.execute(
                f"""
                SELECT latitud, longitud, fecha_registro{extra_ubic}
                FROM ubicaciones
                WHERE vehiculo = %s AND fecha_registro >= %s AND fecha_registro {op_hasta} %s
                ORDER BY fecha_registro ASC
                """,
                (v, desde_g, hasta_g)
            )
        else:
            cursor.execute(
                f"""
                SELECT latitud, longitud, fecha_registro{extra_ubic}
                FROM ubicaciones
                WHERE fecha_registro >= %s AND fecha_registro {op_hasta} %s
                ORDER BY fecha_registro ASC
                """,
                (desde_g, hasta_g)
            )
        filas_crudas = cursor.fetchall()
        truncado = False      # sin tope: se traen todos los envios del grupo

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
                    f"""
                    SELECT id, inicio, fin{extra_viaje}
                    FROM viajes
                    WHERE vehiculo = %s AND inicio <= %s AND (fin IS NULL OR fin >= %s)
                    ORDER BY inicio ASC
                    """,
                    (v, hasta_g, desde_g)
                )
                viajes_db = cursor.fetchall()
            elif v == VEHICULO_POR_DEFECTO:
                cursor.execute(
                    f"""
                    SELECT id, inicio, fin{extra_viaje}
                    FROM viajes
                    WHERE inicio <= %s AND (fin IS NULL OR fin >= %s)
                    ORDER BY inicio ASC
                    """,
                    (hasta_g, desde_g)
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
                'vehiculo': v,
                # Resultados de ML (None si la columna no existe o el vehiculo no los envia)
                'estilo': fila.get('estilo'),
                'evento': fila.get('evento'),
                'eventos': fila.get('eventos')
            })

        # Solo se informan los viajes que realmente tienen puntos en la consulta.
        usados = set(ids)
        for viaje in viajes_db:
            if viaje['id'] in usados:
                item = {
                    'id': viaje['id'],
                    'vehiculo': v,
                    'inicio': formatear_marca(viaje['inicio']),
                    'fin': formatear_marca(viaje['fin']) if viaje['fin'] else None,
                    'sin_cierre': viaje['fin'] is None,
                    # Numero en todo el rango (None sin agrupar: la pagina numera sola)
                    'numero': numeros.get(viaje['id'])
                }
                # Score y desglose (None si no hay columnas o el viaje no tiene ML).
                # DECIMAL llega como Decimal: se pasa a float para el JSON.
                for c in COLUMNAS_SCORE_VIAJE:
                    valor = viaje.get(c)
                    item[c] = float(valor) if isinstance(valor, Decimal) else valor
                viajes.append(item)
        por_vehiculo[v] = {'total': len(filas), 'truncado': truncado}

    cursor.close()
    conexion.close()
    info_grupos = [{
        'numero': i + 1,
        'viajes': g['viajes'],
        'desde': formatear_marca(g['desde']),
        'hasta': formatear_marca(g['hasta'])
    } for i, g in enumerate(grupos)]
    return puntos, viajes, por_vehiculo, {'grupo': grupo, 'grupos': info_grupos}

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

# --- Driving score del viaje ---
def calcular_score_viaje(vehiculo, viaje_id):
    """Calcula y guarda en viajes el score de un viaje ya cerrado.

    driving_score = promedio de las calificaciones de 10 s que envio el vehiculo
    durante el viaje (columna ubicaciones.calificacion). Los puntos sin calificacion
    (NULL: detenido, sin OBD o el celular) no cuentan. Si no hay ninguna, queda NULL.
    Se calcula para TODOS los viajes, tambien los de menos de SCORE_KM_MINIMOS km;
    la pagina decide si lo muestra. score_estilo y score_eventos ya no se usan (NULL).
    Se guardan tambien la distancia, el % de cada estilo y los conteos de eventos
    (todas las maniobras de cada tramo, leidas de la columna eventos).
    """
    try:
        if not tiene_columnas_score_viajes() or not tabla_tiene_vehiculo('ubicaciones'):
            return
        con_ml = tiene_columnas_ml_ubicaciones()
        conexion = conectar_mysql()
        cursor = conexion.cursor(dictionary=True)
        cursor.execute("SELECT id, inicio, fin FROM viajes WHERE id = %s", (viaje_id,))
        viaje = cursor.fetchone()
        if not viaje or not viaje['fin']:
            cursor.close(); conexion.close()
            return
        extra = ((", estilo, evento" if con_ml else "") + (", calificacion" if tiene_columna_calificacion() else "")
                 + (", eventos" if tiene_columna_eventos() else ""))
        cursor.execute(
            f"""
            SELECT latitud, longitud, fecha, hora_col{extra} FROM ubicaciones
            WHERE vehiculo = %s AND fecha_registro BETWEEN %s AND %s
            ORDER BY fecha_registro ASC
            """,
            (vehiculo, viaje['inicio'], viaje['fin'] + TOLERANCIA_FIN_VIAJE)
        )
        # Sin duplicados: si el mismo mensaje se guardara dos veces, la hora GPS
        # (fecha + hora_col) es identica; se deja una sola fila por instante.
        filas, vistos = [], set()
        for f in cursor.fetchall():
            clave = (f['fecha'], f['hora_col'])
            if clave not in vistos:
                vistos.add(clave)
                filas.append(f)

        # Distancia del viaje (suma de tramos entre puntos consecutivos)
        metros, previo = 0.0, None
        for f in filas:
            try:
                punto = (float(f['latitud']), float(f['longitud']))
            except (TypeError, ValueError):
                continue
            if previo:
                metros += distancia_metros(*previo, *punto)
            previo = punto
        km = round(metros / 1000, 2)

        estilos = [f.get('estilo') for f in filas if f.get('estilo') in ('cauteloso', 'normal', 'agresivo')]
        hay_ml = any(f.get('estilo') or f.get('evento') or f.get('calificacion') is not None for f in filas)

        if not hay_ml:
            # Viaje sin ML (celular): solo la distancia.
            cursor.execute("UPDATE viajes SET distancia_km = %s WHERE id = %s", (km, viaje_id))
            conexion.commit()
            cursor.close(); conexion.close()
            print(f"--> Viaje {viaje_id} (vehiculo {vehiculo}) sin ML: {km} km, sin score.")
            return

        # Todas las maniobras de cada tramo (columna eventos; evento en puntos viejos)
        eventos = [e for f in filas for e in lista_eventos(f)]
        n_acel = eventos.count('aceleracion_brusca')
        n_fren = eventos.count('frenado_brusco')
        n_giros = eventos.count('giro_derecha_brusco') + eventos.count('giro_izquierda_brusco')

        pct = {'cauteloso': None, 'normal': None, 'agresivo': None}
        if estilos:
            n = len(estilos)
            pct = {s: round(100 * estilos.count(s) / n, 2) for s in pct}

        # Score del viaje: promedio de las calificaciones de 10 s (sin formula).
        califs = [float(f['calificacion']) for f in filas if f.get('calificacion') is not None]
        score = sum(califs) / len(califs) if califs else None
        score_estilo = score_eventos = None     # columnas de la formula anterior: ya no se usan

        def r(x):
            return None if x is None else int(round(x))

        cursor.execute(
            """
            UPDATE viajes SET driving_score = %s, score_estilo = %s, score_eventos = %s,
                pct_cauteloso = %s, pct_normal = %s, pct_agresivo = %s,
                n_aceleraciones = %s, n_frenados = %s, n_giros = %s, distancia_km = %s
            WHERE id = %s
            """,
            (r(score), r(score_estilo), r(score_eventos),
             pct['cauteloso'], pct['normal'], pct['agresivo'],
             n_acel, n_fren, n_giros, km, viaje_id)
        )
        conexion.commit()
        cursor.close()
        conexion.close()
        print(f"--> Score del viaje {viaje_id} (vehiculo {vehiculo}): {r(score)} "
              f"(promedio de {len(califs)} calificaciones, {km} km)")
    except Exception as error:
        print(f"Error calculando el score del viaje: {error}")

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

    # grupo=1 (por defecto) son los 10 viajes mas recientes del rango, 2 los 10 anteriores...
    texto_grupo = request.args.get('grupo', '1')
    if not texto_grupo.isdigit() or int(texto_grupo) < 1:
        return jsonify({'error': 'El parametro grupo debe ser un numero desde 1'}), 400

    try:
        puntos, viajes, por_vehiculo, info_grupos = consultar_historial(
            desde_utc, hasta_utc, vehiculos, int(texto_grupo))
    except Exception as error:
        print(f"Error consultando el historial: {error}")
        return jsonify({'error': 'No se pudo consultar la base de datos'}), 500

    return jsonify({
        'puntos': puntos,
        'viajes': viajes,
        'total': len(puntos),
        'truncado': any(info['truncado'] for info in por_vehiculo.values()),
        'por_vehiculo': {str(v): info for v, info in por_vehiculo.items()},
        'grupo': info_grupos['grupo'],
        'grupos': info_grupos['grupos']
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

# --- Emulador del Auto 1 (solo en la instancia de pruebas) ---
# La pagina /emulador imita a la ESP32 del Auto 1: arma el MISMO mensaje que
# envia la placa y lo manda por UDP a este mismo servidor, asi pasa por
# udp_listener igual que un dato real (se guarda, se emite en vivo, abre y
# cierra viajes y calcula el score). Solo se activa con EMULADOR=1 en el .env:
# ponerlo SOLO en dev, para no llenar la base de produccion de viajes falsos.
EMULADOR_ACTIVO = os.getenv('EMULADOR', '0').strip() == '1'
# Clave para acceder a /emulador y /api/emulador. Si esta vacia, el acceso
# queda bloqueado por seguridad incluso si EMULADOR=1.
EMULADOR_CLAVE = os.getenv('EMULADOR_CLAVE', '').strip()
EMULADOR_COOKIE = 'emulador_auth'
EMULADOR_VEHICULO = 1
# Destinos, igual que la ESP32 (que envia a los 4 servidores):
#   'local' -> solo esta instancia (127.0.0.1, su propio UDP_PORT)
#   'dev'   -> las 4 instancias, puerto UDP de dev
#   'prod'  -> las 4 instancias, puerto UDP de produccion (como la placa real)
EMULADOR_HOSTS = [h.strip() for h in os.getenv(
    'EMULADOR_HOSTS',
    'felipegps.duckdns.org,carlosgps.duckdns.org,davidgps.duckdns.org,alejandrogps.duckdns.org'
).split(',') if h.strip()]
EMULADOR_PUERTOS = {'dev': int(os.getenv('EMULADOR_PUERTO_DEV', '5001')),
                    'prod': int(os.getenv('EMULADOR_PUERTO_PROD', '5000'))}

def destinos_emulador(destino):
    """Lista de (host, puerto) a los que se envia, segun el destino elegido."""
    if destino == 'local':
        return [('127.0.0.1', UDP_PORT)]
    if destino in EMULADOR_PUERTOS:
        return [(h, EMULADOR_PUERTOS[destino]) for h in EMULADOR_HOSTS]
    raise ValueError('destino debe ser local, dev o prod')

def armar_mensaje_emulador(d):
    """Arma el texto UDP con el formato de la ESP32 a partir de lo que envia la
    pagina. Lanza ValueError si algun dato no es valido."""
    tipo = d.get('tipo')
    if tipo in ('inicio', 'fin'):
        marca = 'INICIO_RECORRIDO' if tipo == 'inicio' else 'FIN_RECORRIDO'
        return f"{marca}\nVehiculo: {EMULADOR_VEHICULO}\nOrigen: emulador"
    if tipo != 'ubicacion':
        raise ValueError('tipo debe ser inicio, fin o ubicacion')
    lat, lon = float(d['lat']), float(d['lon'])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise ValueError('coordenadas fuera de rango')
    estilo = d.get('estilo', 'sin_dato')
    if estilo not in ESTILOS:
        raise ValueError('estilo invalido')
    eventos = d.get('eventos') or []
    if not isinstance(eventos, list) or len(eventos) > MAX_EVENTOS_TRAMO or not all(e in MANIOBRAS for e in eventos):
        raise ValueError('eventos invalidos')
    calificacion = d.get('calificacion')
    if calificacion is not None:
        calificacion = float(calificacion)
        if not 0 <= calificacion <= 100:
            raise ValueError('calificacion fuera de 0-100')
    ahora = datetime.now(UTC)
    return '\n'.join([
        'Ubicacion GPS',
        f'Latitud: {lat:.6f}',
        f'Longitud: {lon:.6f}',
        f"Hora UTC: {ahora.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Hora Colombia: {ahora.astimezone(BOGOTA).strftime('%Y-%m-%d %H:%M:%S')}",
        f'Vehiculo: {EMULADOR_VEHICULO}',
        f'Estilo: {estilo}',
        f"Evento: {eventos[-1] if eventos else 'ninguno'}",
        f"Calificacion: {'sin_dato' if calificacion is None else f'{calificacion:.1f}'}",
        f"Eventos: {','.join(eventos) if eventos else 'ninguno'}",
        # Asi el servidor distingue al emulador de la ESP32 (las versiones viejas la ignoran)
        'Origen: emulador',
    ])

def _token_emulador():
    return hmac.new(EMULADOR_CLAVE.encode('utf-8'), b'emulador-ok', 'sha256').hexdigest()

def emulador_autenticado():
    if not EMULADOR_CLAVE:
        return False
    cookie = request.cookies.get(EMULADOR_COOKIE, '')
    return bool(cookie) and hmac.compare_digest(cookie, _token_emulador())

@app.route('/emulador', methods=['GET', 'POST'])
def emulador():
    if not EMULADOR_ACTIVO:
        return 'No encontrado', 404
    if not EMULADOR_CLAVE:
        return render_template('emulador_login.html', nombre_pagina=NOMBRE_PAGINA,
                               sin_clave=True, error=None), 403
    if request.method == 'POST':
        clave = request.form.get('clave', '')
        if hmac.compare_digest(clave, EMULADOR_CLAVE):
            resp = redirect('/emulador')
            resp.set_cookie(EMULADOR_COOKIE, _token_emulador(),
                            httponly=True, samesite='Lax', max_age=8 * 3600, path='/')
            return resp
        return render_template('emulador_login.html', nombre_pagina=NOMBRE_PAGINA,
                               sin_clave=False, error='Clave incorrecta.'), 401
    if not emulador_autenticado():
        return render_template('emulador_login.html', nombre_pagina=NOMBRE_PAGINA,
                               sin_clave=False, error=None)
    # Con UBICACION_DESDE=emulador el estilo y los eventos vienen de la ESP32:
    # la pagina oculta esos controles y solo emula la ruta.
    return render_template('emulador.html', nombre_pagina=NOMBRE_PAGINA,
                           solo_ruta=(UBICACION_DESDE == 'emulador'))

@app.route('/emulador/salir', methods=['POST'])
def emulador_salir():
    if not EMULADOR_ACTIVO:
        return 'No encontrado', 404
    resp = redirect('/emulador')
    resp.delete_cookie(EMULADOR_COOKIE, path='/')
    return resp

@app.route('/api/emulador', methods=['POST'])
def api_emulador():
    if not EMULADOR_ACTIVO:
        return jsonify({'error': 'El emulador no esta activo en esta instancia'}), 404
    if not emulador_autenticado():
        return jsonify({'error': 'Acceso no autorizado. Inicia sesion en /emulador'}), 401
    datos = request.get_json(silent=True) or {}
    try:
        mensaje = armar_mensaje_emulador(datos)
        destinos = destinos_emulador(datos.get('destino', 'local'))
    except (KeyError, TypeError, ValueError) as error:
        return jsonify({'error': f'Dato invalido: {error}'}), 400
    # UDP no confirma la llegada: "enviado" = el paquete salio hacia ese servidor.
    resultado = []
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        for host, puerto in destinos:
            try:
                sock.sendto(mensaje.encode('utf-8'), (host, puerto))
                resultado.append({'host': host, 'puerto': puerto, 'ok': True})
            except OSError as error:            # p. ej. el dominio no resuelve
                resultado.append({'host': host, 'puerto': puerto, 'ok': False, 'error': str(error)})
    finally:
        sock.close()
    if not any(r['ok'] for r in resultado):
        return jsonify({'error': 'No se pudo enviar a ningun destino', 'destinos': resultado}), 502
    return jsonify({'ok': True, 'mensaje': mensaje, 'destinos': resultado})

if __name__ == '__main__':
    print(f"Página: {NOMBRE_PAGINA}")
    print(f"Ubicacion del Auto 1 desde: {UBICACION_DESDE} (UBICACION_DESDE en el .env)")
    if EMULADOR_ACTIVO:
        print("Emulador del Auto 1 ACTIVO en /emulador (EMULADOR=1 en el .env)")
        estado_clave = "protegido con EMULADOR_CLAVE" if EMULADOR_CLAVE else "BLOQUEADO (falta EMULADOR_CLAVE en el .env)"
        print(f"  -> Acceso a /emulador: {estado_clave}")
    socketio.start_background_task(udp_listener)
    socketio.start_background_task(vigilar_inactividad)
    print(f"Iniciando servidor Web en el puerto {WEB_PORT}...")
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, allow_unsafe_werkzeug=True)