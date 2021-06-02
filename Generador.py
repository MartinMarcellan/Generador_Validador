import random
import sqlite3
import csv
import numpy as np

miconexion = sqlite3.connect("appdatabase") #Nombre de la BBDD
micursor = miconexion.cursor()
formato_tabla = ['x','y','sample number','angle'] #Cabecera del csv resultante

query_mapas = "SELECT localMapId FROM \"Map\" M WHERE M.localMapId not in (SELECT id FROM FutureAction WHERE `scope` = 'MAP' and `action` = 'DELETE')"
micursor.execute(query_mapas) #Query para obtener el número de mapas
datos_mapas = micursor.fetchall()

lista_mapas = ()
for mapa in datos_mapas:
    lista_mapas += (mapa[0],)


query_muestras = "SELECT numOfSamples FROM DataCollectionCampaign WHERE localMapId in "+str(lista_mapas)
micursor.execute(query_muestras) #Query para obtener el número de muestras
numero_muestras = micursor.fetchall()

cantidad_samples = 0

for muestra in numero_muestras:
    cantidad_samples += muestra[0]



query_estaciones = "SELECT l2address,technology,baseStationId FROM BaseStation"
micursor.execute(query_estaciones) #Query para obtener datos de las APs
estaciones = micursor.fetchall()

estacionesconformato = [] #Para unir a la cabecera
direcciones = [] # Para realizar busquedas
ids = () #para filtrar


for estacion in estaciones: #Bucle para formatear correctamente las MAC y nombres con la tecnología de cada AP
    if estacion[1] == 'WIFI' or estacion[1] == 'WIFI-RTT':
        texto = str(estacion[0])+'#'+str(estacion[1])
        estacionesconformato.append(texto)
        direcciones.append(str(estacion[0]))
        ids += (estacion[2],)

formato_tabla.extend(estacionesconformato) #Cabecera extendida definitiva

while True: #Bucle en el que se selecciona cuantas muestras se dedican a cada dataset

    print('Bienvenido al generador de datasets, lo primero es dividir las muestras en dos datasets: testing y training\nLa BBDD dispone de '+str(cantidad_samples)+' muestras, seleccione cuantos quiere incluir en el dataset de training')
    numero_training = int(input())
    numero_testing = cantidad_samples - int(numero_training)

    if numero_training == cantidad_samples or numero_training == 0 or cantidad_samples < numero_training:
        print('ERROR: Introduzca un número de muestras válido\n')
        continue

    print('Ha seleccionado dedicar '+str(numero_training)+' muestras al training y '+str(numero_testing)+' al testing')
    print('Hay que dividir el dataset de testing en dos: testing y validation, ¿Desea dividirlos por porcentaje o número de muestras? (%/#)')
    decision = input()
    if decision == '%':
        porcentaje = input('Teclee el porcentaje de muestras que desea destinar a la validacion (0-1)\n')
        if 0 > float(porcentaje) > 1 or porcentaje==0 or porcentaje==1:
            print('ERROR: Introduzca una porcentaje del 0 al 1 ambos no incluidos\n')
            continue
        numero_validation = numero_testing * float(porcentaje)
        numero_validation = int(round(numero_validation))
    else:
        numero_validation = int(input('Teclee el número de muestras que desea destinar a la validacion (1-'+str(numero_testing)+')\n'))
        if numero_validation==0 or numero_validation==numero_testing:
            print('ERROR: Introduzca un número de muestras válido\n')
            continue
    numero_test_def = numero_testing-numero_validation

    print('Ha seleccionado dedicar '+str(numero_validation)+' muestras a la validacion y '+str(numero_test_def)+' al testing')
    break


division1 = list(str(random.random())) #se genera un número aleatorio de 17 elementos y se hace una lista
del division1[1] #Elmina la coma ya que es un número decimal

#listas en las que se meterán los datos
lista_training = []
lista_testing = []


query_batches = "SELECT MAX(batchId) FROM sample"
micursor.execute(query_batches) #Query para obtener el número de batches en la BBDD
max_batches = micursor.fetchall()[0][0]

#Selección del valor por defecto de la tabla
inicializacion = input('Desea inicializar con algún valor concreto los datasets? (s/n)\n En caso negativo tomarán el valor de -100\n')
if inicializacion == 's' or inicializacion == 'S':
    valor_por_defecto = int(input('Introduce el valor por defecto'))
else:
    valor_por_defecto = -100

print('Generando datasets.')

contador_training = 0; contador_testing = 0; n = 3; indiceb = 1; indices = 0; testing_terminado = False;training_terminado = False
#Se llenan las listas lista_training y lista_testing
while testing_terminado == False or training_terminado == False: #Mientras no se asigne el número indicado a cada lista sigue llenando
    limite_racha = int(division1[n]) + 1 #cuantos samples seguidos para cada lista. Es un número aleatorio
    racha = 0
    if training_terminado == False:
        while racha != limite_racha:
            querytraining = "SELECT C.x,C.y,D.angle,S.value,X.l2address FROM Sample S, Batch B, DataCollectionCampaign D, Coordinate C, BaseStation X WHERE S.batchId == "+str(indiceb)+" AND S.sampleNumber == "+str(indices)+" AND S.batchId == B.batchId AND B.localDataCollectionCampaignId == D.localDataCollectionCampaignId AND D.coordinateId == C.coordinateId AND S.baseStationId == X.baseStationId AND S.baseStationId IN " + str(ids)
            micursor.execute(querytraining) #Query para obtener los datos necesarios para la tabla
            sample_train = micursor.fetchall()

            lista_formateada = valor_por_defecto*np.ones(len(formato_tabla)) #Genera un vector de valores por defecto
            for i in range(3):
                lista_formateada[i] = sample_train[0][i] # Se añaden las coordenadas y el ángulo del punto
            for sample in sample_train: #Bucle para sustituir el valor por defecto con el valor real
                indicedireccion = direcciones.index(sample[4]) + 4 #Se busca por dirección MAC y se obtiene el índice en la lista
                lista_formateada[indicedireccion] = sample[3]

            lista_training.append(lista_formateada) #Se añade la lista formateada a la lista definitiva
            contador_training += 1 #Cuenta el numero de muestras que se añaden a la lista
            racha += 1 #Cuenta las muestras seguidas
            indices += 1 #indice del numero de muestra
            if contador_training == numero_training: #Verifica si ha añadido las muestras asignadas a la lista
                training_terminado = True
                print('training terminado')
                break
            elif indices == 50: #Si el indice de samples llega a 50 se resetea y se suma uno al indice de batches.
                indices = 0
                indiceb += 1
    racha = 0
    if testing_terminado == False:
        while racha != limite_racha:
            querytesting ="SELECT C.x,C.y,D.angle,S.value,X.l2address FROM Sample S, Batch B, DataCollectionCampaign D, Coordinate C, BaseStation X WHERE S.batchId == "+str(indiceb)+" AND S.sampleNumber == "+str(indices)+" AND S.batchId == B.batchId AND B.localDataCollectionCampaignId == D.localDataCollectionCampaignId AND D.coordinateId == C.coordinateId AND S.baseStationId == X.baseStationId AND S.baseStationId IN " + str(ids)
            micursor.execute(querytesting)
            sample_test = micursor.fetchall()

            lista_formateada = valor_por_defecto*np.ones(len(formato_tabla))
            for i in range(3):
                lista_formateada[i] = sample_test[0][i]
            for sample in sample_test:
                indicedireccion = direcciones.index(sample[4]) + 4
                lista_formateada[indicedireccion] = sample[3]

            lista_testing.append(lista_formateada)
            contador_testing += 1
            racha += 1
            indices += 1
            if contador_testing == numero_testing:
                testing_terminado =True
                print('testing terminado')
                break
            elif indices == 50:
                indices = 0
                indiceb += 1
    n += 1
    if n == 17:
        n = 0

print('Generando datasets..')

division2 = list(str(random.random())) #otro número aleatorio
del division2[1]

validation_terminado = False; lista_validation = []; test_bien_terminado = False; lista_test_definitiva = []; contador_val = 0; contador_tst = 0;
m = 0 #Contador para recorrer el número aleatorio

#Se divide la lista_trainig en lista lista_validation y lista_test_definitiva
for sample in lista_training:
    limite_racha = int(division2[m]) + 1 #cuantos samples seguidos para cada lista. Es un número aleatorio
    racha = 0
    if validation_terminado == False:
        while racha != limite_racha:
            lista_validation.append(sample)
            contador_val += 1
            racha += 1
            if contador_val == numero_validation: #Verifica si se han asignado las muestras correspondientes
                validation_terminado = True
                break
    racha = 0
    if test_bien_terminado == False:
        while racha != limite_racha:
            lista_test_definitiva.append(sample)
            contador_tst += 1
            racha += 1
            if contador_tst == numero_test_def: #Verifica si se han asignado las muestras correspondientes
                test_bien_terminado =True
                break
    m += 1
    if m == 17: #reinicia el contador del número aleatorio
        m = 0


print('Generando datasets...')


#Bucler para generar los .csv
with open('training.csv', 'w', newline='') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(formato_tabla)
    n = 0
    for line in lista_training:
        if len(line) < 4:
            wr.writerow(lista)
        else:
            lista = [line[0], line[1], n, line[2]]
            lista.extend(line[3:])
            wr.writerow(lista)
        n += 1

print('training.csv generado')

with open('validation.csv', 'w', newline='') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(formato_tabla)
    n = 0
    for line in lista_validation:
        if len(line) < 4:
            wr.writerow(lista)
        else:
            lista = [line[0], line[1], n, line[2]]
            lista.extend(line[3:])
            wr.writerow(lista)
        n += 1

print('validation.csv generado')

with open('testing.csv', 'w', newline='') as myfile:
    wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
    wr.writerow(formato_tabla)
    n = 0
    for line in lista_test_definitiva:
        if len(line) < 4:
            wr.writerow(lista)
        else:
            lista = [line[0], line[1], n, line[2]]
            lista.extend(line[3:])
            wr.writerow(lista)
        n += 1

print('testing.csv generado')