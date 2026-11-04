# Preconteo electoral · Parte de Sara Mideros

Entrega de `feature/verificacion-infra`: verificación distribuida, presencia, métricas,
consolas «Noche de escrutinio» e infraestructura. Basada en el arranque de `main`
(`187e7fc`). Los módulos de ingesta, resultados públicos y fan-out pertenecen a los
otros integrantes y no se incluyen en esta entrega. El ZIP recibido se consultó
como referencia de contratos; no se incorpora ni se publica.

## Ejecutar con Docker

Requisitos: Docker Desktop con motor Linux y Compose.

```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps
```

Si `.env` ya existe, edítalo sin sobrescribirlo. Accesos locales de ejemplo:

| Consola | URL | Credencial de demostración |
|---|---|---|
| Auditor | http://localhost:8080/auditor | `sara-local` o `auditor-local` |
| Operación | http://localhost:8080/ops | `ops-local` |

Las credenciales de las consolas son independientes de las credenciales de Git.
`.env` está ignorado por Git y por Docker; Compose inyecta las variables al arrancar.
Antes de exponer el despliegue, sustituye las credenciales de ejemplo y termina HTTPS
en el proxy del alojamiento. Redis solo es accesible desde la red interna de Compose.
Compose escucha en localhost por defecto; para un host de despliegue, configura
`HTTP_BIND` con la interfaz correspondiente y el acceso HTTPS del proxy externo.
Los ocho procesos comparten Redis; cada uno tiene un `INSTANCE_ID` diferente.

Para cargar doce casos demostrativos:

```powershell
docker compose exec app1 node scripts/seed-demo.js
```

Los ids `DEMO-E14-*` permiten probar cola, reservas, rechazo y liberación. No son
actas válidas para el módulo de ingesta definitivo. La aprobación queda deshabilitada
hasta integrar `approveHeld`; nunca simula sumar votos.

```powershell
docker compose logs --tail 50 app1 nginx
docker compose restart nginx # también después de recrear las instancias
docker compose stop
docker compose start
docker compose down
```

`down` conserva el volumen Redis; no uses `down -v` si deseas conservar los datos.

## Ejecutar sin Docker para Node

Requisitos: Node 22.9 o posterior, npm y un Redis 7 accesible. Redis puede estar
instalado en Linux/WSL o en otro equipo. El balanceador local usa exclusivamente
módulos nativos de Node, sin nginx.

```powershell
npm ci
Copy-Item .env.example .env
# Ajusta REDIS_URL en .env si Redis no está en 127.0.0.1:6379.
npm run cluster:local
```

Accede por `http://localhost:8080`. Son ocho procesos en los puertos 3101–3108.
`Ctrl+C` cierra el balanceador y sus hijos. Para una sola instancia: `npm start`,
puerto 3000. En otra terminal: `npm run seed`.

Variables adicionales del balanceador: `INSTANCES`, `FIRST_PORT`, `LB_PORT`.
Ejemplo en PowerShell:

```powershell
$env:INSTANCES='2'
npm run cluster:local
```

## Verificaciones

```powershell
npm test
npm run check:deployment # con las ocho instancias arriba; requiere npm ci
# Redis de prueba aislado (no se usa el volumen del despliegue):
docker run -d --name sara-redis-test -p 127.0.0.1:6389:6379 redis:7.4-alpine
$env:TEST_REDIS_URL='redis://127.0.0.1:6389'
npm run test:integration
docker rm -f sara-redis-test
```

Las pruebas de integración fallan si no hay Redis; no sustituyen Redis por un mock.
Usan un prefijo UUID y eliminan únicamente sus claves. Verifican 40 tomas simultáneas,
renovación, expiración, dueño tardío, prioridad, rechazo trazable, presencia por sesión,
Pub/Sub entre dos instancias, SSE de operación y recuperación tras desconexión.
`npm test` comprueba además que la UI funciona en modo degradado y que las métricas
exigen autenticación.

Simulador de 120 conexiones de auditoría (comparte las identidades de `.env`):

```powershell
npm run load:auditors
```

Un tercio corta su conexión abruptamente después de reservar un caso. Los demás lo
liberan. No aprueba ni rechaza actas reales. `AUDITORS`, `DURATION_MS` y `WS_URL`
permiten ajustar la carga. Este simulador muestra comportamiento, no certifica una
capacidad de producción ni reemplaza mediciones en nube.

## Integración con el equipo

Consulta [docs/INTEGRACION.md](docs/INTEGRACION.md). `src/config.js` y `src/geo.js`
del arranque se conservaron. `src/support` es el soporte mínimo para ejecutar esta
entrega antes de que exista `src/redis.js`; no reemplaza los módulos de ingesta.
El servidor expone puntos de extensión para las rutas, gateways y procesos de
fan-out. No se crearon páginas de jurado o de resultados que pertenecen a otros.

## Git: identidad local y acceso temporal

La rama de entrega es `feature/verificacion-infra`.
El remoto `origin` apunta al repositorio indicado por el usuario. La configuración
local vacía `credential.helper` para no reutilizar ni guardar credenciales en el
gestor global, y activa `credential.useHttpPath`.

Cuando vayas a hacer tus propios commits, ajusta tu identidad **solo aquí**:

```powershell
git config --local user.name "TU_NOMBRE"
git config --local user.email "TU_CORREO"
git config --show-origin --get user.name
git config --show-origin --get user.email
git config --local --show-origin --get-all credential.helper
git config --local --show-origin --get credential.useHttpPath
```

Para un futuro push, `git -c credential.helper= push -u origin feature/verificacion-infra`
solicita usuario y token en la terminal sin almacenarlos. No pegues tokens en el
remoto, scripts o archivos versionados. GitHub requiere un token para HTTPS, no la
contraseña de tu cuenta. La identidad del commit (nombre y correo) no concede
permisos de escritura: la cuenta autenticada debe tener acceso al repositorio.

## Guion de sustentación (5–7 minutos)

1. Abre ambas consolas; muestra las ocho instancias y el estado de Redis.
2. Con dos auditores, intenta tomar el mismo caso: solo uno obtiene la reserva.
3. Libera el caso y tómalo desde el otro auditor: todos ven el cambio por Pub/Sub.
4. Rechaza un caso DEMO indicando el motivo; muestra el Stream `verify:audit`.
5. Explica la diferencia entre conexión viva y actividad humana. Sin actividad,
   cesan las renovaciones; al vencer el TTL el caso vuelve a estar disponible.
6. Muestra las pruebas de dueño tardío: un token antiguo no renueva, libera ni rechaza
   la reserva de otro. La aprobación debe validar el mismo token dentro de su Lua.
7. Detén una instancia; nginx distribuye nuevas conexiones entre las restantes.
   Las conexiones ya establecidas a esa instancia se cortan y reconectan con jitter.
8. Detén Redis en el entorno de demostración: `/healthz` retorna 503, la UI informa
   indisponibilidad y no confirma decisiones. Al recuperarlo, la cola se reconstruye.

Arquitectura y límites: [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md).
