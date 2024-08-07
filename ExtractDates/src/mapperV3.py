#!/bin/python3
import sys
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timezone
import warnings

# Suprimir advertencias de solicitudes HTTPS no verificadas
from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.3'
}

def extraer_titulos_y_enlaces(url):
    response = requests.get(url, headers=headers, verify=False)
#    soup = BeautifulSoup(response.content, 'html.parser')
    soup = BeautifulSoup(response.content, 'lxml') 
    enlaces = soup.find_all('a')
    noticias = []
    for enlace in enlaces:
        titulo = enlace.get_text().strip()
        href = enlace.get('href')
        if titulo and href:
            href = urljoin(url, href)
            noticias.append({'titulo': titulo, 'enlace': href})
    return noticias

def convertir_a_iso8601(fecha):
    try:
        dt = datetime.strptime(fecha, '%Y-%m-%dT%H:%M:%S')
        return dt.isoformat()
    except ValueError:
        pass

    match = re.match(r'(\d{1,2}) de (\w+) de (\d{4}), (\d{1,2}):(\d{2}) (AM|PM)', fecha)
    if match:
        print(match)
        dia, mes_texto, anio, hora, minuto, periodo = match.groups()
        meses = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        mes = meses[mes_texto.lower()]
        hora = int(hora)
        dt = datetime(int(anio), mes, int(dia), hora, int(minuto))
        return dt.isoformat()

    match = re.match(r'(\d{1,2}) de (\w+) de (\d{4}) \((\d{1,2}):(\d{2}) h\.\)', fecha)
    if match:
        dia, mes_texto, anio, hora, minuto = match.groups()
        meses = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        mes = meses[mes_texto.lower()]
        dt = datetime(int(anio), mes, int(dia), int(hora), int(minuto))
        return dt.isoformat()

    match = re.match(r'Publicado el (\d{2})/(\d{2})/(\d{4}) a las (\d{1,2})h(\d{2})', fecha)
    if match:
        dia, mes, anio, hora, minuto = match.groups()
        dt = datetime(int(anio), int(mes), int(dia), int(hora), int(minuto))
        return dt.isoformat()

    match = re.match(r'(\d{1,2}):(\d{2}) ET\((\d{1,2}):(\d{2}) GMT\) (\d{1,2}) (\w+), (\d{4})', fecha)
    if match:
        hora_et, minuto_et, hora_gmt, minuto_gmt, dia, mes_texto, anio = match.groups()
        meses = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        mes = meses[mes_texto.lower()]
        dt = datetime(int(anio), mes, int(dia), int(hora_gmt), int(minuto_gmt), tzinfo=timezone.utc)
        return dt.isoformat()

    match = re.match(r'(\d{4})-(\d{2})-(\d{2})', fecha)
    if match:
        anio, mes, dia = match.groups()
        dt = datetime(int(anio), int(mes), int(dia))
        return dt.isoformat()

    return None

def extraer_fecha(url):
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')
    date_attributes = [ 'dateNote','data-date', 'datetime', 'date-publish', 'published', 'time', 'timestamp']
    for attr in date_attributes:
        fecha_elemento = soup.find(attrs={attr: True})
        if fecha_elemento:
            return fecha_elemento.get(attr)
    posibles_clases_fecha = [ 'dateNote', 'date-publish', 'content-time', 'published', 'timestamp', 'time','date']
    for clase in posibles_clases_fecha:
        fecha_elemento = soup.find('div', class_=clase) or soup.find('span', class_=clase) or soup.find('time', class_=clase) or soup.find('time')
        if fecha_elemento and fecha_elemento.get_text():
            if fecha_elemento.get_text().strip() != ' ':
                return fecha_elemento.get_text().strip()
    return None

if __name__ == "__main__":
    for url in sys.stdin:
        url = url.strip()
        if not url:
            continue
        noticias = extraer_titulos_y_enlaces(url)
        for noticia in noticias:
            fecha = extraer_fecha(noticia['enlace'])
            if fecha:
                fecha_correction = convertir_a_iso8601(fecha)
                noticia['fecha'] = fecha_correction
                try:
                    print(f"{noticia['titulo']}\t{noticia['enlace']}\t{noticia.get('fecha', '')}")
                except UnicodeEncodeError:
                    continue
