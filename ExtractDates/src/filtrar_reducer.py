#!/usr/bin/env python3
import sys
import json

# Definir el umbral de relevancia
threshold = 2

# Lista para almacenar las noticias relevantes
relevant_news_list = []

# Conjunto para almacenar combinaciones únicas de titulo y enlace
unique_news_set = set()

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
    cantidad_palabras = len(titulo.split())
    
    # Crear una combinación única de titulo y enlace
    unique_key = (titulo, enlace)
    
    # Agregar el campo de relevancia basado en la cantidad de palabras en el título
    news['relevancia'] = cantidad_palabras
    
    if cantidad_palabras >= threshold and unique_key not in unique_news_set:
        relevant_news_list.append(news)
        unique_news_set.add(unique_key)

# Imprimir todas las noticias relevantes
for news in relevant_news_list:
    print(json.dumps(news, ensure_ascii=False))