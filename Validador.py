import sqlite3
import numpy as np
import statistics
import itertools
from collections import Counter

miconexion = sqlite3.connect("appdatabase") #Nombre de la BBDD
micursor = miconexion.cursor()

query_moviles = "SELECT * FROM UserEquipment"
micursor.execute(query_moviles) #Query para obtener información sobre los móviles utilizados
datos_moviles = micursor.fetchall()
num_moviles = len(datos_moviles)

query_mapas = "SELECT M.localMapId,M.mapName,M.buildingName,M.floorNumber FROM \"Map\" M WHERE M.localMapId not in (SELECT id FROM FutureAction WHERE `scope` = 'MAP' and `action` = 'DELETE')"
micursor.execute(query_mapas) #Query para obtener información sobre los mapas en los que se han tomado medidas
datos_mapas = micursor.fetchall()

ids_validas_mapas = [] #Vector con las Ids de los mapas
for mapa in datos_mapas:
    ids_validas_mapas.append(mapa[0])

while True: #Bucle para seleccionar el mapa y movil que se quieren validar
    print('Bienvenido al validador de datos, seleccione el móvil del cual quiere ver los datos:\n')
    for movil in datos_moviles:
        texto = str(movil[0]) + '=>'+str(movil[1]) + ' ' + str(movil[2])
        print(texto)

    id_movil = input()
    if num_moviles < int(id_movil) or 0 >= int(id_movil):
        print('ERROR: Introduce un número válido\n')
        continue

    print('Seleccione el mapa del que quiera visualizar datos:\n')


    for mapa in datos_mapas:
        texto = str(mapa[0]) + '=> mapa: '+str(mapa[1])+' edificio: '+str(mapa[2])+' piso: '+str(mapa[3])
        print(texto)
    mapa_id = int(input())
    if mapa_id not in ids_validas_mapas:
        print('ERROR: Introduce un número válido\n')
        continue
    break

query_maps = "SELECT * FROM Map WHERE localMapId == "+ str(mapa_id)
micursor.execute(query_maps) #Query para obtener los datos del mapa seleccionado
datosmapa = micursor.fetchall()
nombre_mapa = datosmapa[0][2]
edificio_mapa = datosmapa[0][3]
piso_mapa = datosmapa[0][4]
path_imagen_mapa = '/Android/data/edu.upc.grxca.wifimapbuilder/files/Maps' + datosmapa[0][5]

query_movil = "SELECT brand,model FROM UserEquipment WHERE id == "+str(id_movil)
micursor.execute(query_movil) #Query para obtener los datos del móvil seleccionado
datosmovil = micursor.fetchall()
marcamodelo = str(datosmovil[0][0])+'-'+str(datosmovil[0][1])

#Escribe los datos del móvil y el mapa en el fichero resultado
fichero = 'Report_'+str(nombre_mapa)+'_'+marcamodelo+'.txt' # Crea el fichero
linea0 = 'Informe sobre las medidas tomadas en el mapa '+str(nombre_mapa)+' con el móvil '+str(marcamodelo)+'\n'
linea1 = 'El mapa '+str(nombre_mapa)+' se encuentra en el piso '+str(piso_mapa)+' del edificio '+str(edificio_mapa)+'\n'
linea2 = 'Se puede encontrar la imagen del mapa en el fichero: '+str(path_imagen_mapa)+'\n'

w = open(fichero,'a') # Escribe las lineas creadas
w.write(linea0)
w.write(linea1)
w.write(linea2)

query_nummedidas = "SELECT DISTINCT coordinateId FROM DataCollectionCampaign WHERE localMapId == "+str(mapa_id)
micursor.execute(query_nummedidas) # Query para obtener la lista de coordenadas del mapa seleccionado
medidas_mapa = len(micursor.fetchall())

query_campanias = "SELECT * FROM DataCollectionCampaign WHERE localMapId == "+str(mapa_id)+" AND userEquipmentId == "+str(id_movil)+" ORDER BY coordinateId ASC;"
micursor.execute(query_campanias) #Query para obtener datos sobre las medidas realizadas en el mapa
campanias = micursor.fetchall()
muestras_punto = []
campanias_punto = []
id_puntos = ()

w.write('Se han realizado medidas en '+str(medidas_mapa)+' puntos diferentes de este mapa sumando un total de '+str(len(campanias))+' campañas de medida\n')
w.write('***Anomalías en el número de muestras tomadas:***\n')
w.write('Se considerará anomalía todo punto en el que el número de medidas no se corresponda con el número de campañas\n')

for medida in campanias: #Bucle para generar tres vectores de información
    if medida[2] not in id_puntos:
        id_puntos += (medida[2],) #Ids de los puntos del mapa
        muestras_punto.append(medida[7]) #Número de muestras tomadas en cada punto
        campanias_punto.append(1) #Número de campañas realizadas en cada punto
    else:
        muestras_punto[-1] += medida[7]
        campanias_punto[-1] += 1

query_coordenadas = "SELECT x,y,z FROM Coordinate WHERE coordinateId IN "+ str(id_puntos)
micursor.execute(query_coordenadas) #Query para obtener las coordenadas de cada punto del mapa
coordenadas = micursor.fetchall()

contador = 0
for punto in coordenadas: #Bucle para destacar en el fichero los puntos que no cumplen 50 muestras por campaña
    if (muestras_punto[contador]/campanias_punto[contador])%50 != 0:
        w.write('En el punto con coordenadas '+str(punto)+'se han recogido '+ str(muestras_punto[contador])+ ' muestras en '+str(campanias_punto[contador])+' campañas'+'\n')
    contador +=1


micursor.execute("SELECT * FROM BaseStation")
estaciones = micursor.fetchall() #Query para obtener la lista de APs

idRTT = () #Lista con IDs de los APs RTT
idRSS = () #Lista con IDs de los APs RSS
for est in estaciones:
    if est[3] == 'WIFI-RTT':
        idRTT += (est[0],)
    elif est[3] == 'WIFI':
        idRSS += (est[0],)

lista_larga = (idRTT,idRSS)

def estadisticas(lista, mapa_id, porcentaje, punto_id): #función para obtner la media y la desviación en un punto IN: lista de Ids de los AP, Id del mapa, porcentaje umbral, Ids de los puntos del mapa
    query_estadisticas = "SELECT sampleNumber,baseStationId,angle FROM Sample S, Batch B, DataCollectionCampaign D WHERE D.localMapId == " + str(mapa_id) + " AND D.coordinateId == " + str(punto_id) + " AND D.userEquipmentId == " + str(id_movil) + " AND D.localDataCollectionCampaignId == B.localDataCollectionCampaignId AND B.batchId == S.batchId AND S.baseStationId IN " + str(lista)
    micursor.execute(query_estadisticas) #Query para obtener los datos de la medida
    samples_mapa = micursor.fetchall()
    if samples_mapa == []: #En caso de error se devuelve esto para que sea fácilmente identificable
        return 0,1
    n = 0
    #Crea dos vectores de cincuenta coordenadas para almacenar según su sampleNumber las baseStationId detectadas
    samples0 = [[] for _ in range(50)]
    samples1 = [[] for _ in range(50)]

    while n < len(samples_mapa): #Bucle para añadir las muestras con ángulo x al vector samplesx
        if samples_mapa[n][2] == 1:
            samples1[samples_mapa[n][0]].append(samples_mapa[n][1])
        else:
            samples0[samples_mapa[n][0]].append(samples_mapa[n][1])
        n += 1

    batches =[samples0,samples1]

    #Vector de dos coordenadas donde se guardan las estaciones base a eliminar
    eliminar = [[] for _ in range(2)]
    bat = 0

    while bat < 2:
        unida = itertools.chain.from_iterable(batches[bat]) #Une en una lista todas las baseStationId que tienen el mismo ángulo
        cuentas = Counter(unida) #Genera una lista en la que se cuenta cuantas veces aparece cada elemento
        for elemento in cuentas:
            if cuentas[elemento] / 50 < porcentaje: #Si el elemento no cumple con el porcentaje es añadido a la lista de eliminación
                eliminar[bat].append(elemento)
        bat += 1

    unido = [[] for _ in range(2)] #Une todos los elementos de cada batch para hacer la media y eliminar los que no cumplen porcentaje
    l = 0
    for elemento in batches:
        for e in elemento:
            unido[l] += e
        l += 1


    m = 0
    for batch in eliminar: #Elimina las baseStationId que no cumplen con el porcentaje
        for numero in batch:
            unido[m].remove(numero)
        m += 1

    longitudes = [] #Vector en el que cada una de las dos coordenadas está el número de APs detectados en el porcentaje indicado de las muestras
    for batch in unido:
        APs_unicos = len(Counter(batch).keys()) #Aañde las IDs al vector
        longitudes.append(APs_unicos)

    if longitudes == []: #En caso de que no cumpla ninguno se devuelve esto para que sea fácilmente identificable
        return 0,2

    media = statistics.mean(longitudes) #Calcula media entre ángulo 0 y 1
    desv = np.std(longitudes) #Calcula la desviación típica

    return media, desv

w.write('***Anomalías en dispositivos detectados:***\n')
w.write('Se considerarán anomalías todos los puntos en los que no se detecta ningún dispositivo o en los que la desviación típica sea mayor a 0\n')
w.write('1)----Detección de dispositivos RTT----\n')
n = 0

#lista_larga = [RTT,RSS]

for lista in lista_larga: #Bucle para escribir en el fichero los puntos que son considerados anómalos
    contador1 = 0
    for punto in id_puntos:
        media, desv = estadisticas(lista, mapa_id, 0, punto) #Se ejecuta la función para obtener los datos
        if desv != 0: #Aquí se puede regular la condición para que un punto sea considerado anómalo
            if media == 0: #La media es 0 en las dos excepciones indicadas en la función
                w.write('En el punto con coordenadas ' + str(coordenadas[contador1]) + ' se ha detectado un total de 0 dispositivos\n')
            else:
                w.write('En el punto con coordenadas ' + str(coordenadas[contador1]) + ' se ha detectado una media de ' + str(media) + ' dispositivos RTT con una desviación típica de ' + str(desv)+'\n')
        contador1 += 1
    if n == 0:
        w.write('2)----Detección de dispositivos RSS----\n') #La segunda coordenada es RSS y este if se hace para escribir la linea de título.
        n += 1

respuesta = input('Desea aplicar un porcentaje umbral para filtrar los dispositivos que se detecten en dicho porcentaje de muestras? (s/n)\n')

if respuesta == 's' or respuesta == 'S':
    while True: #Bucle que hace lo mismo que el anterior pero aplicando el porcentaje umbral
        porcentaje = input('Que porcentaje desea aplicar? (0-1)\n')
        if float(porcentaje) < 1 and float(porcentaje) > 0:
            w.write('***Anomalías en dispositivos detectados con filtrado del '+str(porcentaje)+':***\n')
            w.write('1)----Detección de dispositivos RTT----\n')
            n = 0
            for lista in lista_larga:
                contador1 = 0
                for punto in id_puntos:
                    media, desv = estadisticas(lista, mapa_id, float(porcentaje), punto)
                    if desv != 0:
                        if media == 0:
                            w.write('En el punto con coordenadas ' + str(coordenadas[contador1]) + ' se ha detectado un total de 0 dispositivos\n')
                        else:
                            w.write('En el punto con coordenadas ' + str(coordenadas[contador1]) + ' se ha detectado una media de ' + str(media) + ' dispositivos RTT con una desviación típica de ' + str(desv) + '\n')
                    contador1 += 1
                if n == 0:
                    w.write('2)----Detección de dispositivos RSS----\n')
                    n += 1
            break
        else:
            continue
print('Puede consultar el resultados del iforme en el fichero '+fichero)