# Informe de arquitectura — Preconteo en tiempo real

Taller de WebSockets · Programación Orientada a la Web · Séptimo semestre
Grupo: Juan Felipe · Juan Cajigas · Sara

> Las cifras marcadas como **medido** salen de ejecuciones reales del repositorio (clúster local de 8 instancias + Redis 7). Antes de la sustentación hay que repetirlas contra el despliegue en la nube y reemplazarlas en la tabla de la sección 7.

---

## 2. Flujo de un acta

1. El jurado digita el acta; la app la **persiste en localStorage** antes de enviarla (RT-6).
2. Viaja por WebSocket con su `idem`. La instancia valida formato y regla contable.
3. `apply_acta.lua` hace, en una sola operación atómica: idempotencia → control de versión → reversión de la versión anterior → suma en mesa/puesto/municipio/departamento → conteo de mesas informadas → marca de tópicos sucios → registro de la corrección.
4. La instancia responde al jurado con un estado inequívoco (`APPLIED`, `CORRECTED`, `DUPLICATE`, `STALE`, `HELD`, `UNAVAILABLE`…).
5. Cada `TICK_MS` el líder ejecuta `flush_window.lua`: calcula el delta mínimo por tópico y hace `XADD`.
6. Las 8 instancias reciben la entrada y hacen fan-out por lotes a los grupos de vista afectados.

## 3. Requerimientos funcionales

### RF-1 · Idempotencia
- Clave `idem:<clave>` con la **respuesta original** serializada, `EX 6 h` (`IDEM_TTL_SEC`). Un reenvío recibe `DUPLICATE` + la respuesta original: el jurado sabe con certeza qué pasó.
- Segunda barrera independiente de la clave: la **versión** guardada en `acta:<id>`. Si la clave ya expiró y llega un reintento tardío, el script ve `version actual ≥ recibida` y responde `DUPLICATE_VERSION`/`STALE` sin sumar.
- **Caída entre registrar y confirmar:** el script es atómico, así que o se aplicó todo (incluida la clave) o nada. Si la instancia muere después del `EVALSHA` pero antes de enviar el ack, el jurado no recibe respuesta, su app reenvía a los 8 s con **la misma clave**, cae en otra instancia y recibe `DUPLICATE` con el resultado original. Nunca se suma dos veces ni se pierde.

### RF-2 · Correcciones y convergencia
- Regla: se aplica una versión solo si es **estrictamente mayor** que la aplicada. El estado final es `max(versión válida recibida)`, independiente del orden.
- La reversión usa los votos guardados de la versión anterior (`ZINCRBY -n`) y suma los nuevos, todo dentro del mismo script.
- Las correcciones se encolan en `corrections:pending`; el agregador las adjunta a la ventana y el público las ve en "Cambios explicados", con el diferencial por candidato. Así un número que baja **siempre** llega con su explicación.
- Una versión dudosa queda en `held:<id>` y no toca el acumulado hasta que un auditor la aprueba; si mientras tanto llega una versión válida mayor, la retenida queda obsoleta (`superseded`).

### RF-3 · Acumulados jerárquicos atómicos
- Totales en **ZSET** `tot:{mesa|puesto|mun|dep}` (miembro = opción, score = votos). Ranking sin recorrer nada: `ZREVRANGE tot:mun:11 0 -1 WITHSCORES` es O(log N + M) con M = 6 opciones.
- Mesas informadas en contadores `inf:*` (`INCR` solo la primera vez que una mesa reporta).
- **Por qué Lua y no varios comandos desde Node:** entre dos comandos enviados por la aplicación pueden ejecutarse comandos de otros clientes (otra instancia aplicando otra acta, un lector tomando la fotografía). Un `MULTI/EXEC` evita el intercalado pero no permite decidir con datos leídos dentro de la transacción (versión actual, votos previos). Redis ejecuta un script Lua **de principio a fin sin intercalar nada**, así que ningún lector ve el municipio actualizado y el puesto todavía no.
- Lectura también atómica: `snapshot.lua` lee lo publicado + el id del Stream en la misma operación.
- **Restricciones que impone Lua:** el script bloquea Redis mientras corre (debe ser corto: el nuestro es O(opciones)); no puede hacer I/O externo; en Redis Cluster todas las claves deben declararse en `KEYS` y vivir en el mismo slot (habría que usar hash tags como `{e}tot:dep`). `flush_window.lua` construye claves dentro del script, válido en un nodo con réplica.
