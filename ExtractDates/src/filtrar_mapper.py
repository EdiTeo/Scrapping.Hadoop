#!/usr/bin/env python3

import sys
import json
from datetime import datetime

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        return None

if len(sys.argv) != 4:
    raise ValueError("Debe proporcionar tres argumentos: fecha_inicio, fecha_fin y dominios_permitidos")

fecha_inicio = parse_date(sys.argv[1])
fecha_fin = parse_date(sys.argv[2])
dominios_permitidos = sys.argv[3].split(',')

if fecha_inicio is None or fecha_fin is None:
    raise ValueError("Formato de fecha incorrecto. Use el formato ISO 8601: YYYY-MM-DDTHH:MM:SS")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    
    try:
        news = json.loads(line)
    except json.JSONDecodeError:
        continue

    titulo = news.get('titulo', '')
    enlace = news.get('enlace', '')
    fecha_str = news.get('fecha', '')

    if not fecha_str:
        continue

    fecha_dt = parse_date(fecha_str)
    
    if fecha_dt and fecha_inicio <= fecha_dt <= fecha_fin:
        if any(domain in enlace for domain in dominios_permitidos):
            print(f"{json.dumps(news)}")
