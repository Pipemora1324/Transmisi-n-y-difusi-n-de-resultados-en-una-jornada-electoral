# Contratos de la entrega de Sara

La referencia recibida no se copia ni se publica. Estos contratos permiten unir las
tres ramas conservando la propiedad de cada módulo.

## Archivos compartidos

- `package.json`: conservar las dependencias del arranque (`ws`, `ioredis`) y sumar
  los scripts de las otras ramas al integrar. El lockfile se generó con npm.
- `.env.example`: sumar las variables de ingesta y fan-out; las de esta entrega
  están leídas por `src/support/config.js` para no modificar `src/config.js` ajeno.
- `src/server.js`: `startServer` acepta `extensions`, `upgrades`, `startExtensions`
  y `approveHeld`. Importar módulos de otras ramas únicamente después de integrarlos.
- `src/http/routes.js`: las extensiones retornan `true` si atendieron la petición.
  Mantener las rutas públicas y de jurados al unir los cambios.

Ejemplo orientativo para el servidor integrado (ajusta las firmas de cada rama):

```js
await startServer({
  approveHeld,
  upgrades: { '/ws/jurado': jurorWss },
  extensions: [async (req, res, url) => {
    if (url.pathname === '/sse/public') {
      await handlePublicSse(req, res, url);
      return true;
    }
    return false;
  }],
  startExtensions: [async () => {
    startDispatcher();
    startTicker();
    return async () => { /* detener los procesos integrados */ };
  }],
});
```

El arranque standalone detecta `src/ingest/actas.js` si existe. Otra ruta se configura
con `APPROVE_MODULE`. No incluye ni reimplementa `approveHeld`: es parte de Juan Felipe.
La función debe retornar `{status}` y comprobar el token en la misma operación Lua
que aplica los votos; un chequeo previo desde Node no garantiza exclusión.

## Redis compartido

Con `REDIS_PREFIX` vacío se usan los nombres de la referencia:

| Clave | Tipo | Contrato |
|---|---|---|
| `queue:verify` | ZSET | miembro = id de acta; score mayor = más prioridad |
| `held:<id>` | HASH | `version`, `mesa`, `puesto`, `municipio`, `votos` JSON, `sufragantes`, `motivo`, `ts`, datos de ingesta |
| `lock:acta:<id>` | STRING + TTL | token opaco `auditorId:UUID`, compatible con comparación exacta en `apply_acta.lua` |
| `claims` | HASH | id → JSON `{auditor,since}`; no contiene el token |
| `ch:audit` | Pub/Sub | aviso de cambio; al recibirlo se vuelve a leer Redis |
| `verify:active` | ZSET | id → vencimiento del lease (reloj Redis) |
| `verify:presence` | ZSET | sesión de socket → vencimiento (reloj Redis) |
| `verify:people` | HASH | sesión → identidad y caso visible |
| `verify:audit` | STREAM | rechazos, dueño, motivo; retención aproximada de 10.000 entradas |
| `metrics:<instanceId>` | STRING + TTL | fotografía JSON de métricas de proceso |
| `verify:metric-instances` | ZSET | registro de instancias y expiración |

Una reserva conserva el miembro de la cola: `claimedBy` lo identifica como ocupado.
Al expirar el lock, ya puede retomarse aunque el barrido no haya ejecutado; el barrido
retira el reclamo visual y el índice auxiliar. La ingesta elimina cola, hash retenido,
lock y reclamo cuando resuelve o reemplaza un acta. Puede quedar un índice auxiliar
de reserva hasta su próximo vencimiento; no autoriza operaciones ni resucita casos.

Los scripts de Sara se ejecutan vía `runLua`/`EVAL`, con sus propias listas de KEYS.
Si la rama de ingesta registra comandos ioredis como `claimActa`, `renewLock`,
`releaseLock` o `rejectActa` con otras firmas, deben retirarse esos registros o
actualizarse al integrar. Los módulos de Sara no usan esos comandos.

`REDIS_PREFIX` sirve para aislar pruebas y despliegues; para integrarse con la ingesta
de la referencia se deja vacío. Si se usa prefijo en producción, todas las ramas
deben aplicar el mismo a **todas** sus claves y canales. Redis Cluster no está
soportado: los scripts operan sobre varias claves de un único servidor Redis.

## WebSocket `/ws/auditor`

1. Conectar y enviar en menos de 5 s: `{type:"auth",token:"…"}`.
2. Recibir `hello`, luego `state` con `items`, `people`, `redis`.
3. Enviar `claim` con `id` opcional, `release`, `reject` con `reason`, `approve`.
4. Recibir `result` con `action` y `status`. Solo `claim/OK` incluye `case` y `expiresAt`.
5. Cada 5 s enviar `activity` si existe interacción humana reciente y la pestaña está
   visible; en otro caso `heartbeat`. Solo `activity` renueva el caso.

El token de la reserva queda en el servidor, nunca en el cliente ni en Pub/Sub.
Se serializan mensajes por socket para evitar dos reservas simultáneas desde una
misma conexión. La identidad viene de credenciales de entorno, nunca de un parámetro
`auditor` enviado por el navegador. Un mismo auditor puede abrir varias conexiones;
la presencia se agrupa por identidad sin que cerrar una elimine las demás.

## Operación

`GET /api/ops` y `GET /sse/ops` exigen `Authorization: Bearer <OPS_TOKEN>`.
SSE emite `event: metrics` y `data: <JSON>` cada intervalo. El frontend usa `fetch`
streaming para enviar la cabecera, sin tokens en query strings. Desconecta clientes
que acumulen más de 128 KiB y limita a 200 streams por proceso.

Los contadores adicionales de ingesta/fan-out pueden incorporarse con
`metrics.increment(nombre, cantidad)`. No se inventan tasas de actas, INP o ahorro
de mensajes antes de integrar la instrumentación de las otras ramas.

Para las importaciones de las otras ramas se conserva una fachada nombrada
`metrics` con `actaApplied`, `conn`, `naive`, `emitted`, `publishLatency`, `lagging`
y `droppedSlow`. Se vincula al recolector del proceso al arrancar el servidor.
Las métricas adicionales permanecen en cero/sin muestras mientras no existan
productores reales; la latencia usa hasta 1.000 observaciones por intervalo.

## Versiones retenidas y aprobación

El contrato heredado de `approveHeld(id, token)` aprueba la versión actualmente
retenida. Si ingesta permite reemplazar el hash `held` mientras existe una reserva,
se debe ampliar la operación de aprobación para comprobar **atómicamente** también
la versión revisada, o invalidar el lock cuando cambie esa versión. Esta entrega
no modifica `apply_acta.lua` ajeno ni afirma resolver ese contrato de ingesta.
La aprobación está deshabilitada en esta rama hasta que se integre ese módulo.
