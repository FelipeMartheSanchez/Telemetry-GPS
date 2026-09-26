import os
import math
import socket
import mysql.connector
from datetime import datetime
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

# --- Lugar: radio de busqueda, fijo en el codigo (el usuario no lo elige) ---
RADIO_LUGAR_METROS = 100

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

def guardar_en_mysql(datos):
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        print("Faltan variables de base de datos en el .env. No se guarda el registro.")
        return
    try:
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        consulta = """
            INSERT INTO ubicaciones (latitud, longitud, fecha, hora_col)
            VALUES (%s, %s, %s, %s)
        """
        valores = (datos['latitud'], datos['longitud'], datos['fecha'], datos['hora_col'])
        cursor.execute(consulta, valores)
        conexion.commit()
        cursor.close()
        conexion.close()
        print("--> Registro guardado en MySQL (AWS RDS) exitosamente.")
    except Exception as error:
        print(f"Error al conectar/guardar en MySQL: {error}")

# --- Viajes: marcas de inicio y fin de recorrido ---
# Se disparan cuando la app Android manda los mensajes cortos
# "INICIO_RECORRIDO" / "FIN_RECORRIDO" (distintos al bloque de 5 lineas
# de una coordenada normal), justo cuando el usuario presiona los botones
# Iniciar/Finalizar. No se infieren los limites del viaje revisando huecos
# de tiempo: el propio usuario confirma cuando empieza y cuando termina.
def registrar_inicio_viaje():
    """Crea una fila nueva en viajes al recibir INICIO_RECORRIDO. fin queda NULL."""
    try:
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute("INSERT INTO viajes (inicio) VALUES (NOW())")
        conexion.commit()
        cursor.close()
        conexion.close()
        print("--> Viaje iniciado.")
    except Exception as error:
        print(f"Error al registrar inicio de viaje: {error}")

def registrar_fin_viaje():
    """Cierra el viaje abierto mas reciente (fin IS NULL) al recibir FIN_RECORRIDO."""
    try:
        conexion = conectar_mysql()
        cursor = conexion.cursor()
        cursor.execute("""
            UPDATE viajes SET fin = NOW()
            WHERE fin IS NULL
            ORDER BY inicio DESC
            LIMIT 1
        """)
        conexion.commit()
        filas_afectadas = cursor.rowcount
        cursor.close()
        conexion.close()
        if filas_afectadas == 0:
            print("--> FIN_RECORRIDO recibido, pero no habia ningun viaje abierto.")
        else:
            print("--> Viaje finalizado.")
    except Exception as error:
        print(f"Error al registrar fin de viaje: {error}")

def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', UDP_PORT))
    print(f"Escuchando mensajes UDP en el puerto {UDP_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        mensaje = data.decode('utf-8').strip()

        # Marcas de inicio/fin de viaje: mensajes cortos, distintos al
        # bloque de 5 lineas que trae una coordenada normal. Se revisan
        # primero para no intentar interpretarlas como una ubicacion.
        if mensaje == "INICIO_RECORRIDO":
            registrar_inicio_viaje()
            continue
        elif mensaje == "FIN_RECORRIDO":
            registrar_fin_viaje()
            continue

        lineas = mensaje.split('\n')

        if len(lineas) >= 5:
            crudo_col = lineas[4].replace('Hora Colombia: ', '').strip()
            fecha, hora_col = separar_fecha_hora(crudo_col)

            datos_json = {
                'latitud': lineas[1].replace('Latitud: ', '').strip(),
                'longitud': lineas[2].replace('Longitud: ', '').strip(),
                'fecha': fecha,
                'hora_col': hora_col
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

# --- Historico: consulta de la ventana de tiempo ---
def consultar_historial(desde_utc, hasta_utc):
    """Devuelve los puntos guardados entre dos instantes, en orden cronologico.

    Se filtra por fecha_registro (timestamp) y no por fecha/hora_col, porque
    esas dos son varchar y no se pueden comparar ni ordenar correctamente.
    """
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    consulta = """
        SELECT latitud, longitud, fecha_registro
        FROM ubicaciones
        WHERE fecha_registro BETWEEN %s AND %s
        ORDER BY fecha_registro ASC
        LIMIT %s
    """
    cursor.execute(consulta, (desde_utc, hasta_utc, LIMITE_PUNTOS))
    filas = cursor.fetchall()
    cursor.close()
    conexion.close()

    puntos = []
    for fila in filas:
        # fecha_registro sale de MySQL en UTC y sin zona: se la marcamos
        # y la traducimos a hora de Colombia para mostrarla.
        marca = fila['fecha_registro'].replace(tzinfo=UTC).astimezone(BOGOTA)
        puntos.append({
            'lat': float(fila['latitud']),
            'lon': float(fila['longitud']),
            'hora': marca.strftime('%d/%m/%Y %I:%M:%S %p')
        })
    return puntos

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
def buscar_viaje_de_visita(entrada_utc, salida_utc):
    """Devuelve (inicio, fin) del viaje que abarca esta visita, o None si no
    hay ningun viaje cerrado que la contenga (por ejemplo, datos guardados
    antes de que la app empezara a mandar INICIO_RECORRIDO/FIN_RECORRIDO).

    Solo se consideran viajes ya CERRADOS (fin IS NOT NULL): la comparacion
    "fin >= %s" en SQL descarta automaticamente las filas con fin NULL,
    porque NULL nunca es mayor o igual a nada.
    """
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    cursor.execute("""
        SELECT inicio, fin FROM viajes
        WHERE inicio <= %s AND fin >= %s
        ORDER BY inicio DESC
        LIMIT 1
    """, (entrada_utc, salida_utc))
    fila = cursor.fetchone()
    cursor.close()
    conexion.close()
    return fila

# --- Lugar: agrupa los puntos cercanos en "visitas" ---
def encontrar_visitas(lat_lugar, lon_lugar):
    """Recorre todo el historial de ubicaciones en orden cronologico y agrupa
    los puntos consecutivos que caen dentro de RADIO_LUGAR_METROS en una
    sola "visita" (hora de entrada, hora de salida), en vez de devolver cada
    punto suelto. Por cada visita, busca ademas el viaje completo (tabla
    viajes) que la contiene, para que el frontend pueda dibujar la ruta
    entera del viaje al hacer clic, no solo el tramo cercano al lugar.
    """
    conexion = conectar_mysql()
    cursor = conexion.cursor(dictionary=True)
    cursor.execute("""
        SELECT latitud, longitud, fecha_registro
        FROM ubicaciones
        ORDER BY fecha_registro ASC
    """)
    filas = cursor.fetchall()
    cursor.close()
    conexion.close()

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
    visitas = []
    for cruda in visitas_crudas:
        entrada_utc = cruda['entrada_utc']
        salida_utc = cruda['salida_utc']

        entrada_local = entrada_utc.replace(tzinfo=UTC).astimezone(BOGOTA)
        salida_local = salida_utc.replace(tzinfo=UTC).astimezone(BOGOTA)

        viaje = buscar_viaje_de_visita(entrada_utc, salida_utc)
        if viaje:
            viaje_inicio_local = viaje['inicio'].replace(tzinfo=UTC).astimezone(BOGOTA)
            viaje_fin_local = viaje['fin'].replace(tzinfo=UTC).astimezone(BOGOTA)
            viaje_inicio_iso = viaje_inicio_local.strftime('%Y-%m-%dT%H:%M:%S')
            viaje_fin_iso = viaje_fin_local.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            viaje_inicio_iso = None
            viaje_fin_iso = None

        visitas.append({
            'entrada': entrada_local.strftime('%d/%m/%Y %I:%M:%S %p'),
            'salida': salida_local.strftime('%d/%m/%Y %I:%M:%S %p'),
            'entrada_iso': entrada_local.strftime('%Y-%m-%dT%H:%M:%S'),
            'salida_iso': salida_local.strftime('%Y-%m-%dT%H:%M:%S'),
            'puntos': cruda['puntos'],
            'viaje_inicio': viaje_inicio_iso,
            'viaje_fin': viaje_fin_iso
        })

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

    if not desde or not hasta:
        return jsonify({'error': 'Faltan los parametros desde y hasta'}), 400

    try:
        desde_utc = a_utc(desde)
        hasta_utc = a_utc(hasta)
    except ValueError:
        return jsonify({'error': 'Formato de fecha invalido'}), 400

    if desde_utc >= hasta_utc:
        return jsonify({'error': 'La fecha inicial debe ser anterior a la final'}), 400

    try:
        puntos = consultar_historial(desde_utc, hasta_utc)
    except Exception as error:
        print(f"Error consultando el historial: {error}")
        return jsonify({'error': 'No se pudo consultar la base de datos'}), 500

    return jsonify({
        'puntos': puntos,
        'total': len(puntos),
        'truncado': len(puntos) >= LIMITE_PUNTOS
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

    try:
        visitas = encontrar_visitas(lat_lugar, lon_lugar)
    except Exception as error:
        print(f"Error consultando el lugar: {error}")
        return jsonify({'error': 'No se pudo consultar la base de datos'}), 500

    return jsonify({
        'radio_metros': RADIO_LUGAR_METROS,
        'visitas': visitas,
        'total_visitas': len(visitas)
    })

if __name__ == '__main__':
    print(f"Página: {NOMBRE_PAGINA}")
    socketio.start_background_task(udp_listener)
    print(f"Iniciando servidor Web en el puerto {WEB_PORT}...")
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, allow_unsafe_werkzeug=True)