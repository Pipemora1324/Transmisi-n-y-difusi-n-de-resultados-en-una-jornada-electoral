# Arquitectura · Responsabilidad de Sara

## 1. Topología

Navegadores → nginx round-robin → ocho procesos Node idénticos → Redis compartido.
Cada proceso sirve HTTP, WebSocket de auditoría y SSE de operación. Redis concentra
las reservas, cola, presencia y fotografías de métricas. No hay comunicación directa
entre instancias. El proceso retiene solo sockets locales, estado de la reserva de
su conexión y muestras de instrumentación.

El estado de ingesta, acumulados e historial público corresponde a los otros dos
integrantes. No se incluyen aquí sus implementaciones ni sus secciones del informe.

## 2. Ingesta e idempotencia

Pendiente de integración de Juan Felipe. Sara consume el contrato `held:<acta>` y
delega la aprobación a `approveHeld`; no suma ni revierte votos desde Node.

## 3. Requisitos funcionales de Sara

### RF-7 · Verificación y exclusión distribuida

La cola es un ZSET de mayor prioridad primero, con desempate lexicográfico de Redis.
Una toma explícita cuesta O(log N) para consultar la cola más operaciones sobre el
hash y el lock. La toma automática recorre como máximo 200 candidatos para evitar
scripts de duración no acotada; una cola con los primeros 200 ocupados puede requerir
tomar un id explícito. La UI muestra hasta 200 casos, no finge listar toda la cola.

`claim.lua` consulta cola y hash, verifica ausencia de lock, crea el token con TTL,
registra dueño e índice de vencimiento y avisa por Pub/Sub en una sola ejecución.
Redis serializa los scripts, por lo que dos procesos no pueden ganar la misma toma.
La reserva tiene un UUID nuevo por adquisición. `renew`, `release` y `reject`
comparan el token completo antes de mutar datos. Una respuesta tardía de un dueño
anterior no puede actuar sobre la siguiente reserva, aun si es el mismo auditor.

TTL corto y renovación condicionada resuelven la tensión entre continuidad y
recuperación. La consola reporta actividad humana reciente y visibilidad; el
heartbeat de transporte mantiene presencia, pero no renueva una reserva. Redis
calcula los vencimientos con `TIME`, evitando relojes distintos entre instancias.
La indicación de segundos del navegador es orientativa; la autoridad es Redis.

Un cierre detectado libera la reserva. Si el proceso muere sin ejecutar el cierre,
el TTL la hace reclamable y el barrido limpia su metadato. `sweep.lua` procesa lotes
de 100 y vuelve a verificar `PTTL` antes de retirar un reclamo. Todas las instancias
pueden barrer: la operación es atómica e idempotente y no requiere otro líder.
Rechazar exige motivo de 5–500 caracteres y registra una entrada de auditoría antes
de borrar el retenido. El historial es acotado y no sustituye un archivo probatorio.

### RF-8 · Presencia

Cada socket autenticado obtiene una sesión UUID, independiente de la identidad.
ZSET + HASH mantienen expiración y datos visibles. El listado filtra vencidos usando
el reloj Redis y agrupa identidades. El barrido elimina fantasmas por lotes. Cerrar
una pestaña elimina únicamente su sesión; la otra conexión sigue presente.

Pub/Sub notifica cambios de cola entre instancias, sin trasladar tokens. La consola
recibe fotografías obtenidas de Redis al conectar, ante eventos y cada cinco segundos.
Perder un aviso de Pub/Sub no pierde el estado definitivo. Las lecturas compuestas
de la UI son eventualmente consistentes; las decisiones se protegen en Lua.

## 4. Redis y disponibilidad

Se separan conexión de comandos y conexión de suscripción. Sin cola offline en los
comandos, una caída devuelve `UNAVAILABLE`; la UI no confirma decisiones fallidas.
`/healthz` retorna 503 si Redis no está listo, aunque los archivos de la consola
siguen disponibles para explicar el estado. ioredis reconecta y el gateway vuelve
a suscribirse y reconstruir la fotografía.

Redis usa AOF y volumen persistente en Compose, con política `noeviction` para no
expulsar locks y retenidos por presión de memoria. Sigue siendo un punto único de
fallo: ocho procesos Node no proporcionan alta disponibilidad de Redis. AOF con
la política predeterminada tampoco garantiza durabilidad absoluta ante fallo del
host. Redis de producción requiere respaldo, dimensionamiento y estrategia de HA.

## 5. Sin sesiones pegajosas

Round-robin decide la instancia al abrir cada conexión; WebSocket y SSE permanecen
en esa instancia durante su vida. La reconexión puede llegar a cualquier otra,
porque dueño y cola residen en Redis. Se evitan particiones de carga causadas por
afinidad por IP y dependencia de la memoria de un servidor. A cambio hacen falta
lecturas compartidas, Pub/Sub, autenticación en cada nueva conexión y recuperación
explícita del estado. El estado de edición no enviado del navegador no se replica.

El cliente reconecta con espera exponencial y jitter, hasta unos 30 segundos.
nginx no almacena en buffer SSE y habilita Upgrade de WS; sus timeouts superan los
latidos. El balanceador de Node ofrece la misma topología para desarrollo y retira
puertos de procesos que dejan de responder. No es un sustituto operativo de nginx.

## 6. Instrumentación

`monitorEventLoopDelay` mide un histograma por intervalo (20 ms de resolución).
Se publican p95, máximo, utilización del Event Loop, RSS, uptime, conexiones y
contadores. Cada proceso publica una fotografía con TTL y se registra en un ZSET
de expiración. Los lectores recuperan únicamente instancias vigentes, sin `KEYS *`.

La agregación suma RSS, conexiones y contadores vivos. El mayor p95 por instancia
es una señal de saturación; **no es el p95 global**. Los contadores son de proceso,
se reinician al arrancar y dejan de agregarse al expirar su fotografía. No son
totales históricos de la elección. No se deduce throughput de sus diferencias.

El tablero de operación consume SSE autenticado, conserva 60 muestras y dibuja
historial real de retraso y cola. Una caída de Redis deja los valores como no
disponibles; no muestra datos ficticios ni conserva un estado verde engañoso.

## 7. Infraestructura y seguridad operativa

Docker ejecuta Node como usuario sin privilegios, instala desde lockfile y expone
solo nginx en 8080 (localhost por defecto). Ocho servicios explícitos evitan depender del escalado de Swarm.
Cada proceso tiene healthcheck y apagado con SIGTERM; las conexiones reciben cierre
y el navegador reconecta. Los archivos estáticos están restringidos a `public`.

Las consolas requieren credenciales de entorno: WS autentica en su primer mensaje;
operación usa Authorization. Los tokens quedan en memoria, no en localStorage ni en
URLs. Los mensajes WS están limitados a 8 KiB, 20/s por socket y 500 sockets por
proceso; existe corte por contrapresión. Las credenciales estáticas sirven al
proyecto, pero no reemplazan un proveedor de identidad con revocación y roles.

RT-1, RT-2, RT-3, RT-5, RT-7 y RT-10 están asignados a Sara en WORKPLAN. Como no se
recibió el enunciado que define cada RT, no se inventan equivalencias numéricas:
esta entrega evidencia ocho instancias, proxy sin afinidad, coordinación Redis,
transportes, instrumentación e infraestructura; debe contrastarse con la rúbrica.

## 8. Validación y despliegue

Las pruebas usan Redis real y dos instancias dentro del mismo test. Validan
exclusión, fencing, renovación, TTL, presencia y fan-out de avisos. El servidor
degradado también se prueba sin Redis. El despliegue Compose permite verificar ocho
instancias en `/ops` y alternancia en `/healthz` a través de nginx. El simulador
ejercita hasta 120 conexiones por defecto con desconexiones abruptas.

Para desplegar en un host Linux: instalar Docker/Compose, copiar los archivos de
esta entrega, crear `.env` con secretos propios y ejecutar `docker compose up -d
--build`. Conectar un dominio y terminar TLS delante de nginx. Esta entrega no
contrata nube, no configura DNS y no publica código en GitHub. Las mediciones en
nube y el cumplimiento de umbrales de la rúbrica requieren el sistema integrado.
