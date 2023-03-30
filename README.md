# TP0: Docker + Comunicaciones + Concurrencia

En el presente repositorio se provee un ejemplo de cliente-servidor el cual corre en containers con la ayuda de [docker-compose](https://docs.docker.com/compose/). El mismo es un ejemplo práctico brindado por la cátedra para que los alumnos tengan un esqueleto básico de cómo armar un proyecto de cero en donde todas las dependencias del mismo se encuentren encapsuladas en containers. El cliente (Golang) y el servidor (Python) fueron desarrollados en diferentes lenguajes simplemente para mostrar cómo dos lenguajes de programación pueden convivir en el mismo proyecto con la ayuda de containers.

Por otro lado, se presenta una guía de ejercicios que los alumnos deberán resolver teniendo en cuenta las consideraciones generales descriptas al pie de este archivo.

## Instrucciones de uso
El repositorio cuenta con un **Makefile** que posee encapsulado diferentes comandos utilizados recurrentemente en el proyecto en forma de targets. Los targets se ejecutan mediante la invocación de:

* **make \<target\>**:
Los target imprescindibles para iniciar y detener el sistema son **docker-compose-up** y **docker-compose-down**, siendo los restantes targets de utilidad para el proceso de _debugging_ y _troubleshooting_.

Los targets disponibles son:
* **docker-compose-up**: Inicializa el ambiente de desarrollo (buildear docker images del servidor y cliente, inicializar la red a utilizar por docker, etc.) y arranca los containers de las aplicaciones que componen el proyecto.
* **docker-compose-down**: Realiza un `docker-compose stop` para detener los containers asociados al compose y luego realiza un `docker-compose down` para destruir todos los recursos asociados al proyecto que fueron inicializados. Se recomienda ejecutar este comando al finalizar cada ejecución para evitar que el disco de la máquina host se llene.
* **docker-compose-logs**: Permite ver los logs actuales del proyecto. Acompañar con `grep` para lograr ver mensajes de una aplicación específica dentro del compose.
* **docker-image**: Buildea las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por **docker-compose-up**, por lo cual se lo puede utilizar para testear nuevos cambios en las imágenes antes de arrancar el proyecto.
* **build**: Compila la aplicación cliente para ejecución en el _host_ en lugar de en docker. La compilación de esta forma es mucho más rápida pero requiere tener el entorno de Golang instalado en la máquina _host_.

### Servidor
El servidor del presente ejemplo es un EchoServer: los mensajes recibidos por el cliente son devueltos inmediatamente. El servidor actual funciona de la siguiente forma:
1. Servidor acepta una nueva conexión.
2. Servidor recibe mensaje del cliente y procede a responder el mismo.
3. Servidor desconecta al cliente.
4. Servidor procede a recibir una conexión nuevamente.

### Cliente
El cliente del presente ejemplo se conecta reiteradas veces al servidor y envía mensajes de la siguiente forma.
1. Cliente se conecta al servidor.
2. Cliente genera mensaje incremental.
recibe mensaje del cliente y procede a responder el mismo.
3. Cliente envía mensaje al servidor y espera mensaje de respuesta.
Servidor desconecta al cliente.
4. Cliente vuelve al paso 2.

Al ejecutar el comando `make docker-compose-up` para comenzar la ejecución del ejemplo y luego el comando `make docker-compose-logs`, se observan los siguientes logs:

```
$ make docker-compose-logs
docker compose -f docker-compose-dev.yaml logs -f
client1  | time="2023-03-17 04:36:59" level=info msg="action: config | result: success | client_id: 1 | server_address: server:12345 | loop_lapse: 20s | loop_period: 5s | log_level: DEBUG"
client1  | time="2023-03-17 04:36:59" level=info msg="action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°1\n"
server   | 2023-03-17 04:36:59 DEBUG    action: config | result: success | port: 12345 | listen_backlog: 5 | logging_level: DEBUG
server   | 2023-03-17 04:36:59 INFO     action: accept_connections | result: in_progress
server   | 2023-03-17 04:36:59 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2023-03-17 04:36:59 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°1
server   | 2023-03-17 04:36:59 INFO     action: accept_connections | result: in_progress
server   | 2023-03-17 04:37:04 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2023-03-17 04:37:04 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°2
server   | 2023-03-17 04:37:04 INFO     action: accept_connections | result: in_progress
client1  | time="2023-03-17 04:37:04" level=info msg="action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°2\n"
server   | 2023-03-17 04:37:09 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2023-03-17 04:37:09 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°3
server   | 2023-03-17 04:37:09 INFO     action: accept_connections | result: in_progress
client1  | time="2023-03-17 04:37:09" level=info msg="action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°3\n"
server   | 2023-03-17 04:37:14 INFO     action: accept_connections | result: success | ip: 172.25.125.3
server   | 2023-03-17 04:37:14 INFO     action: receive_message | result: success | ip: 172.25.125.3 | msg: [CLIENT 1] Message N°4
client1  | time="2023-03-17 04:37:14" level=info msg="action: receive_message | result: success | client_id: 1 | msg: [CLIENT 1] Message N°4\n"
server   | 2023-03-17 04:37:14 INFO     action: accept_connections | result: in_progress
client1  | time="2023-03-17 04:37:19" level=info msg="action: timeout_detected | result: success | client_id: 1"
client1  | time="2023-03-17 04:37:19" level=info msg="action: loop_finished | result: success | client_id: 1"
client1 exited with code 0
```

## Parte 1: Introducción a Docker
En esta primera parte del trabajo práctico se plantean una serie de ejercicios que sirven para introducir las herramientas básicas de Docker que se utilizarán a lo largo de la materia. El entendimiento de las mismas será crucial para el desarrollo de los próximos TPs.

### Ejercicio N°1:
Modificar la definición del DockerCompose para agregar un nuevo cliente al proyecto.

### Ejercicio N°1.1:
Definir un script (en el lenguaje deseado) que permita crear una definición de DockerCompose con una cantidad configurable de clientes.

### Ejercicio N°2:
Modificar el cliente y el servidor para lograr que realizar cambios en el archivo de configuración no requiera un nuevo build de las imágenes de Docker para que los mismos sean efectivos. La configuración a través del archivo correspondiente (`config.ini` y `config.yaml`, dependiendo de la aplicación) debe ser inyectada en el container y persistida afuera de la imagen (hint: `docker volumes`).

### Ejercicio N°3:
Crear un script que permita verificar el correcto funcionamiento del servidor utilizando el comando `netcat` para interactuar con el mismo. Dado que el servidor es un EchoServer, se debe enviar un mensaje al servidor y esperar recibir el mismo mensaje enviado. Netcat no debe ser instalado en la máquina _host_ y no se puede exponer puertos del servidor para realizar la comunicación (hint: `docker network`).

### Ejercicio N°4:
Modificar servidor y cliente para que ambos sistemas terminen de forma _graceful_ al recibir la signal SIGTERM. Terminar la aplicación de forma _graceful_ implica que todos los _file descriptors_ (entre los que se encuentran archivos, sockets, threads y procesos) deben cerrarse correctamente antes que el thread de la aplicación principal muera. Loguear mensajes en el cierre de cada recurso (hint: Verificar que hace el flag `-t` utilizado en el comando `docker compose down`).

## Parte 2: Repaso de Comunicaciones

Las secciones de repaso del trabajo práctico plantean un caso de uso denominado **Lotería Nacional**. Para la resolución de las mismas deberá utilizarse como base al código fuente provisto en la primera parte, con las modificaciones agregadas en el ejercicio 4.

### Ejercicio N°5:
Modificar la lógica de negocio tanto de los clientes como del servidor para nuestro nuevo caso de uso.

#### Cliente
Emulará a una _agencia de quiniela_ que participa del proyecto. Existen 5 agencias. Deberán recibir como variables de entorno los campos que representan la apuesta de una persona: nombre, apellido, DNI, nacimiento, numero apostado (en adelante 'número'). Ej.: `NOMBRE=Santiago Lionel`, `APELLIDO=Lorca`, `DOCUMENTO=30904465`, `NACIMIENTO=1999-03-17` y `NUMERO=7574` respectivamente.

Los campos deben enviarse al servidor para dejar registro de la apuesta. Al recibir la confirmación del servidor se debe imprimir por log: `action: apuesta_enviada | result: success | dni: ${DNI} | numero: ${NUMERO}`.

#### Servidor
Emulará a la _central de Lotería Nacional_. Deberá recibir los campos de la cada apuesta desde los clientes y almacenar la información mediante la función `store_bet(...)` para control futuro de ganadores. La función `store_bet(...)` es provista por la cátedra y no podrá ser modificada por el alumno.
Al persistir se debe imprimir por log: `action: apuesta_almacenada | result: success | dni: ${DNI} | numero: ${NUMERO}`.

#### Comunicación:
Se deberá implementar un módulo de comunicación entre el cliente y el servidor donde se maneje el envío y la recepción de los paquetes, el cual se espera que contemple:
* Definición de un protocolo para el envío de los mensajes.
* Serialización de los datos.
* Correcta separación de responsabilidades entre modelo de dominio y capa de comunicación.
* Correcto empleo de sockets, incluyendo manejo de errores y evitando los fenómenos conocidos como [_short read y short write_](https://cs61.seas.harvard.edu/site/2018/FileDescriptors/).
* Límite máximo de paquete de 8kB.

### Ejercicio N°6:
Modificar los clientes para que envíen varias apuestas a la vez (modalidad conocida como procesamiento por _chunks_ o _batchs_). La información de cada agencia será simulada por la ingesta de su archivo numerado correspondiente, provisto por la cátedra dentro de `.data/datasets.zip`.
Los _batchs_ permiten que el cliente registre varias apuestas en una misma consulta, acortando tiempos de transmisión y procesamiento. La cantidad de apuestas dentro de cada _batch_ debe ser configurable.
El servidor, por otro lado, deberá responder con éxito solamente si todas las apuestas del _batch_ fueron procesadas correctamente.

### Ejercicio N°7:
Modificar los clientes para que notifiquen al servidor al finalizar con el envío de todas las apuestas y así proceder con el sorteo.
Inmediatamente después de la notificacion, los clientes consultarán la lista de ganadores del sorteo correspondientes a su agencia.
Una vez el cliente obtenga los resultados, deberá imprimir por log: `action: consulta_ganadores | result: success | cant_ganadores: ${CANT}`.

El servidor deberá esperar la notificación de las 5 agencias para considerar que se realizó el sorteo e imprimir por log: `action: sorteo | result: success`.
Luego de este evento, podrá verificar cada apuesta con las funciones `load_bets(...)` y `has_won(...)` y retornar los DNI de los ganadores de la agencia en cuestión. Antes del sorteo, no podrá responder consultas por la lista de ganadores.
Las funciones `load_bets(...)` y `has_won(...)` son provistas por la cátedra y no podrán ser modificadas por el alumno.

## Parte 3: Repaso de Concurrencia

### Ejercicio N°8:
Modificar el servidor para que permita aceptar conexiones y procesar mensajes en paralelo.
En este ejercicio es importante considerar los mecanismos de sincronización a utilizar para el correcto funcionamiento de la persistencia.

En caso de que el alumno implemente el servidor Python utilizando _multithreading_,  deberán tenerse en cuenta las [limitaciones propias del lenguaje](https://wiki.python.org/moin/GlobalInterpreterLock).

## Consideraciones Generales
Se espera que los alumnos realicen un _fork_ del presente repositorio para el desarrollo de los ejercicios.
El _fork_ deberá contar con una sección de README que indique como ejecutar cada ejercicio.
La Parte 2 requiere una sección donde se explique el protocolo de comunicación implementado.
La Parte 3 requiere una sección que expliquen los mecanismos de sincronización utilizados.

Finalmente, se pide a los alumnos leer atentamente y **tener en cuenta** los criterios de corrección provistos [en el campus](https://campusgrado.fi.uba.ar/mod/page/view.php?id=73393).

## ¿Como ejecutar cada ejercicio?

### Ejercicio N°1:
Simplemente ejecutando make docker-compose-up y make docker-compose-logs se puede ver en la consola como ahora hay dos clientes que intercambian mensajes con el servidor. El cliente original es "client1" y el cliente nuevo es "client2". El id del cliente se indica tambien en el campo "msg".

### Ejercicio N°1.1:
Usage: python3 docker-compose-multiple-clients-generator.py <number of clients> <name of new docker compose>.yaml

Ejecutandose ese comando se crea el archivo pedido. Luego debera reemplazarse el original por este en el Makefile para ejecutar el sistema con la nueva configuración. En caso de llamar al script sin parametros tomara por default 1 cliente (idem docker compose original) y un nombre definido en el .py para el archivo. En caso de mandar solo el parametro de numero de clientes, tomara por default solo el nombre del archivo. Este nombre por default del archivo ya mencionado es "docker-compose-multiple-clients.yaml".

Comentario de implementacion: se decidio que el script copie el archivo original como base para que en caso de querer modificar configuraciones ajenas a la cantidad de clientes, sea mas facil hacerlo solo modificando este archivo original. Si el archivo se creara completo desde el script, habria que modificar dicho script si se quisieran hacer cambios ajenos a la cantidad de clientes.
Esto implica la "limitación" de que el archivo nuevo debe llamarse diferente al archivo docker compose original (docker-compose-dev).

### Ejercicio N°2:
Simplemente ejecutando make docker-compose-up y make docker-compose-logs se puede correr el programa, no hay cambios en la forma de ejecutar.

### Ejercicio N°3:
La forma de ejecutar este ejercicio es make docker-compose-up que levanta servidor (y cliente) y luego para testear el servidor ejecutar make docker-compose-server-test. La palabra enviada es PING asi que debemos recibir PING como respuesta por consola. Si no lo recibimos o recibimos otra cosa, hay problemas con el servidor.

### Ejercicio N°4: 
-t 1 significa que se espera 1 segundo antes de hacer kill al container.
Se ejecuta normalmente el programa, al hacer make docker-compose-down se veran los logs que indican que se manejo correctamente la señal SIGTERM tanto en el cliente como en el servidor.

### Ejercicio N°5: 
Para ejecutar el programa se debe hacer como se venia ejecutando normalmente: make docker-compose-up y make docker-compose-logs para ver los logs. Luego si se ejecuta ' docker exec -it server /bin/bash -c "cat bets.csv" ' puede verse el archivo de apuestas del servidor para corroborar que se hayan cargado correctamente.

Breve explicación del protocolo: primero se envian 4 bytes (siempre esa cantidad) con la cantidad de bytes que ocupara el mensaje (la apuesta) que se enviara a continuación, y luego se envia dicha apuesta en paquetes de bytes de hasta 8kb. Una vez que finaliza la transmisión se envia un ack que consiste siempre en 4 bytes donde puede venir un 1 (procesamiento correcto) o 0 (ocurrio un error). El protocolo contempla posibles short read y short write.
La apuesta se envia con los campos separados por coma ",". Si se enviara mas de una apuesta, se separan con ";".

### Ejercicio N°6: 
La ejecución es analoga al ejercicio 5. Los archivos se leen de .data/dataset/ asi que deben estar ahi adentro, de querer cambiar el nombre se puede hacerlo desde el docker compose.
Si se desea cambiar la cantidad de apuestas que viajan en un batch, se puede hacer desde client_py/config.ini, en el campo BETS_PER_BATCH, que por default esta en 10.
