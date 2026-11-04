# Validación local · 3 de noviembre de 2026

Entorno: Windows, Node 24.19.0, Docker Desktop 28.3.2, Redis 7.4 Alpine.

| Comprobación | Resultado |
|---|---|
| `npm test` | 5 pruebas aprobadas; incluye servidor degradado con Redis inaccesible |
| `npm run test:integration` | 5 pruebas reportadas por Node (grupo + 4 subpruebas), todas aprobadas con Redis real |
| Exclusión | Un solo ganador de 40 tomas simultáneas del mismo caso |
| Fencing | Token anterior rechazado al renovar, liberar y rechazar una reserva nueva |
| Recuperación | Lease renovado, luego vencido; caso reclamable por otro auditor |
| Presencia | Cerrar una sesión no elimina otra; barrido elimina sesiones vencidas |
| Dos instancias | Cola y reclamos visibles por Pub/Sub; SSE de métricas autenticado |
| `docker compose up -d --build` | Ocho procesos saludables, Redis saludable y nginx activo |
| `nginx -t` | Configuración válida |
| `npm run check:deployment` con nginx | Ocho ids distintos en `/healthz`, ocho publicaciones en métricas, rutas y protección de operación correctas |
| Clúster local Node | Ocho procesos en puertos 3201–3208; balanceador 8081 distribuyó a los ocho; se detuvo al finalizar |
| Simulador de auditores, 10 segundos | 120 conexiones, 15 reservas, 10 liberaciones, 5 desconexiones abruptas, 0 errores reportados |
| Consolas en navegador | Auditor autenticado, toma de caso, votos, motivo, presencia y aprobación deshabilitada; operación muestra ocho instancias reales |
| Responsive | 1440, 768 y 390 px; sin desbordamiento horizontal de la página; tabla operativa con desplazamiento dentro de su contenedor |
| Consola del navegador | Sin errores ni advertencias reportados en las dos páginas durante la revisión |
| Git | HEAD original `187e7fc`, rama `feature/verificacion-infra`, sin commits nuevos ni push; configuración de credenciales en `.git/config` |

El Redis de pruebas fue un contenedor desechable en 6389 y se eliminó al terminar.
El clúster de demostración conserva doce casos `DEMO-E14-*` en su volumen Redis.
Las capturas locales se guardan en `artifacts/`, ignorado por Git. El ZIP adjunto
no se extrajo al repositorio. `src/config.js` y `src/geo.js` coinciden con `main`.

Estos resultados validan el alcance de Sara en el entorno local. No validan la
aprobación de votos, las páginas de jurado/público, la ingesta, el fan-out público,
el INP ni el comportamiento en nube de las otras ramas pendientes de integrar.
