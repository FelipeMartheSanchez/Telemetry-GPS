import os
import socket
import mysql.connector
from dotenv import load_dotenv
from flask import Flask, render_template
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

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

def separar_fecha_hora(valor):
    try:
        fecha_str, hora_str = valor.strip().split(' ')
        a, m, d = fecha_str.split('-')
        return f"{d}/{m}/{a}", hora_str.split('.')[0]
    except Exception:
        return valor, valor

def guardar_en_mysql(datos):
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        print("Faltan variables de base de datos en el .env. No se guarda el registro.")
        return
    try:
        conexion = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=5
        )
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

@app.route('/')
def index():
    return render_template('index.html', nombre_pagina=NOMBRE_PAGINA)

if __name__ == '__main__':
    print(f"Página: {NOMBRE_PAGINA}")
    socketio.start_background_task(udp_listener)
    print(f"Iniciando servidor Web en el puerto {WEB_PORT}...")
    socketio.run(app, host='0.0.0.0', port=WEB_PORT, allow_unsafe_werkzeug=True)
