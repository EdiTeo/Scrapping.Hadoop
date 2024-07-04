#!/usr/bin/env python3
import tkinter as tk
from tkinter import messagebox, ttk, scrolledtext
from tkcalendar import DateEntry
from datetime import datetime
import json
import webbrowser
import os

class HadoopApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Ejecución de Hadoop Local")
        self.root.geometry("400x400")

        # Botón para seleccionar archivos y fechas
        tk.Button(self.root, text="Configurar y Ejecutar", command=self.configurar_y_ejecutar).pack()

    def configurar_y_ejecutar(self):
        ventana_configuracion = tk.Toplevel(self.root)
        ventana_configuracion.title("Configurar Ejecución")
        ventana_configuracion.geometry("400x500")

        # Label y calendario para el rango de fechas
        tk.Label(ventana_configuracion, text="Fecha de Inicio:").pack()
        fecha_inicio_cal = DateEntry(ventana_configuracion, width=12, background='darkblue',
                                     foreground='white', borderwidth=2, year=2024)
        fecha_inicio_cal.pack(pady=10)

        tk.Label(ventana_configuracion, text="Fecha de Fin:").pack()
        fecha_fin_cal = DateEntry(ventana_configuracion, width=12, background='darkblue',
                                  foreground='white', borderwidth=2, year=2024)
        fecha_fin_cal.pack(pady=10)

        # Checkboxes para seleccionar los dominios
        self.dominios_seleccionados = {"lostiempos": tk.BooleanVar(), "eldeber": tk.BooleanVar(), "opinion": tk.BooleanVar()}
        tk.Label(ventana_configuracion, text="Seleccionar Dominios:").pack()
        tk.Checkbutton(ventana_configuracion, text="Los Tiempos", variable=self.dominios_seleccionados["lostiempos"]).pack()
        tk.Checkbutton(ventana_configuracion, text="El Deber", variable=self.dominios_seleccionados["eldeber"]).pack()
        tk.Checkbutton(ventana_configuracion, text="Opinión", variable=self.dominios_seleccionados["opinion"]).pack()

        # Botón para confirmar y ejecutar
        tk.Button(ventana_configuracion, text="Ejecutar", command=lambda: self.ejecutar_hadoop(
            fecha_inicio_cal.get_date(), fecha_fin_cal.get_date()
        )).pack()

    def ejecutar_hadoop(self, fecha_inicio, fecha_fin):
        archivo_mapper = "/home/hadoop/data/src/filter_mapper.py"
        archivo_reducer = "/home/hadoop/data/src/filtrado_reducer.py"

        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%dT00:00:00')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%dT23:59:59')
        archivo_entrada = "/BigDataNoticias/noticia.json"
        archivo_salida = f"/BigDataNoticias/noticias-{datetime.now().strftime('%Y%m%d')}"

        # Construir la lista de dominios seleccionados
        dominios_permitidos = [dom for dom, var in self.dominios_seleccionados.items() if var.get()]
        if not dominios_permitidos:
            messagebox.showwarning("Selección Incompleta", "Debe seleccionar al menos un dominio.")
            return

        # Convertir la lista de dominios a una cadena separada por comas
        dominios_str = ','.join(dominios_permitidos)

        # Variable de Hadoop
        os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
        os.environ['PATH'] += f":{os.environ['HADOOP_HOME']}/bin:{os.environ['HADOOP_HOME']}/sbin"

        # Comprobar si la carpeta de salida ya existe y eliminarla si es necesario
        comando_check = f"hadoop fs -test -e {archivo_salida}"
        salida_check = os.system(comando_check)
        if salida_check == 0:
            # La carpeta existe, eliminarla
            comando_delete = f"hadoop fs -rm -r {archivo_salida}"
            os.system(comando_delete)

        comando_hadoop = f"hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar " \
                         f"-files {archivo_mapper},{archivo_reducer} " \
                         f"-mapper 'python3 {archivo_mapper} {fecha_inicio_str} {fecha_fin_str} {dominios_str}' " \
                         f"-reducer 'python3 {archivo_reducer}' " \
                         f"-input {archivo_entrada} -output {archivo_salida}"
        print(comando_hadoop)

        os.system(comando_hadoop)
        self.mostrar_salida_hadoop_fs(archivo_salida)

    def mostrar_salida_hadoop_fs(self, archivo_salida):
        comando_cat = f"hadoop fs -cat {archivo_salida}/part-00000"
        salida = os.popen(comando_cat).read()
        errores = os.popen(f"hadoop fs -ls {archivo_salida}/_SUCCESS").read()
        if not errores.strip():
            messagebox.showerror("Error de Ejecución", "Ocurrió un error al recuperar los datos de HDFS")
        else:
            ventana_salida_hdfs = tk.Toplevel(self.root)
            ventana_salida_hdfs.title("Salida de HDFS")
            ventana_salida_hdfs.geometry("800x400")

            columnas = ("Título", "Enlace", "Fecha", "Relevancia")
            tabla = ttk.Treeview(ventana_salida_hdfs, columns=columnas, show='headings')
            for col in columnas:
                tabla.heading(col, text=col, command=lambda c=col: self.ordenar_por(tabla, c, False))
                tabla.column(col, width=200)

            for linea in salida.splitlines():
                try:
                    json_obj = json.loads(linea)
                    tabla.insert("", "end", values=(json_obj["titulo"], json_obj["enlace"], json_obj["fecha"], int(json_obj.get("relevancia", 0))))
                except json.JSONDecodeError:
                    continue

            tabla.pack(fill=tk.BOTH, expand=True)
            tabla.bind("<Double-1>", lambda event: self.abrir_enlace(tabla, event))

    def ordenar_por(self, tabla, col, descendente):
        datos = [(self.convertir_tipo(tabla.set(child, col)), child) for child in tabla.get_children('')]
        datos.sort(reverse=descendente)

        for index, (val, child) in enumerate(datos):
            tabla.move(child, '', index)

        tabla.heading(col, command=lambda c=col: self.ordenar_por(tabla, c, not descendente))

    def convertir_tipo(self, val):
        try:
            return int(val)
        except ValueError:
            try:
                return float(val)
            except ValueError:
                return val

    def abrir_enlace(self, tabla, event):
        item = tabla.identify('item', event.x, event.y)
        enlace = tabla.item(item, 'values')[1]
        webbrowser.open(enlace)

ventana_principal = tk.Tk()
app = HadoopApp(ventana_principal)
ventana_principal.mainloop()
