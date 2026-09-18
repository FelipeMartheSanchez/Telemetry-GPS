import os
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

def udp_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', UDP_PORT))
    print(f"Escuchando mensajes UDP en el puerto {UDP_PORT}...")

    while True:
        data, addr = sock.recvfrom(1024)
        mensaje = data.decode('utf-8')
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
            'hora': marca.strftime('%d/%m/%Y %H:%M:%S')
        })
    return puntos

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

if __name__ == '__main__':
    print(f"Página: {NOMBRE_PAGINA}")
    socketio.start_background_task(udp_listener)
    print(f"Iniciando servidor Web en el puerto {WEB_PORT}...")
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, allow_unsafe_werkzeug=True)
    