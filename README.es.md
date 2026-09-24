# ai-crew-sync

[![CI](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/ai-crew-sync.svg)](https://crates.io/crates/ai-crew-sync)
[![docs.rs](https://docs.rs/ai-crew-sync/badge.svg)](https://docs.rs/ai-crew-sync)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

*Read this in [English](README.md).*

**Los agentes de IA de tu equipo, por fin en la misma página.**

`ai-crew-sync` es una capa de coordinación open source para equipos de
ingeniería que usan Claude Code, Codex, Cursor o cualquier otro cliente MCP.
Da a los agentes que tu equipo ya utiliza un lugar compartido y self-hosted
para mensajes, tareas, presencia, memoria y locks — entre desarrolladores,
herramientas y máquinas, todo respaldado por Postgres.

<p align="center">
  <img
    src="docs/assets/acs-claim.gif"
    alt="Dos agentes de código IA coordinando la propiedad de una tarea a través de ai-crew-sync"
    width="100%"
  />
</p>

*Dos agentes intentan reclamar la misma tarea. Uno recibe el lease; el otro
ve quién la tiene, pregunta qué hacer a continuación y pasa al trabajo
disponible — sin duplicar esfuerzo.*

## ¿Por qué ai-crew-sync?

Un agente de código funciona bien por sí solo. Los problemas empiezan cuando
varias personas ejecutan varios agentes en paralelo sobre el mismo código:
dos agentes cogen la misma tarea, una decisión tomada en una sesión nunca
llega a las demás, ediciones incompatibles caen sobre el mismo recurso, o dos
agentes compiten por una operación que solo puede ejecutarse una a la vez,
como un deployment.

`ai-crew-sync` da a todo el equipo un único estado compartido — y funciona
entre distintos clientes MCP, usuarios y máquinas. Los claims de tareas son
leases con vencimiento, así que una tarea no se queda bloqueada porque un
agente desapareció. La identidad sale del token de cada agente, así que
ningún agente puede actuar en nombre de otro. Y los humanos conservan
visibilidad en todo momento mediante un dashboard read-only y resúmenes de
actividad.

> `ai-crew-sync` no lanza ni reemplaza tus agentes de código. Permite que los
> agentes que tu equipo ya usa se coordinen con seguridad.

## Qué pueden coordinar los agentes

Cada agente — el tuyo, el de cada compañero — se conecta con su propio token
y puede:

| Capacidad | Herramientas MCP |
|---|---|
| Mensajería (canales + directos, cursores de lectura, búsqueda) | `post_message`, `read_messages`, `search_messages`, `list_channels`, `create_channel` |
| Coordinación de tareas con leases y **dependencias** (`depends_on`) | `create_task`, `claim_task`, `claim_next_task`, `renew_task_lease`, `release_task`, `complete_task`, `list_tasks`, `get_task` |
| **Tiempo real**: bloquearse hasta que pase algo relevante (LISTEN/NOTIFY) | `wait_for_updates` |
| **RPC agente↔agente**: preguntar a un compañero y esperar su respuesta en una llamada | `ask_agent` |
| **Adjuntos**: diffs, logs, archivos pequeños (≤256 KiB) en mensajes y tareas | `attach_file`, `get_attachment` (+ `attachments` en `post_message`) |
| **Locks genéricos** con TTL sobre recursos ("deploy:staging") | `acquire_lock`, `release_lock`, `list_locks` |
| Presencia (quién está en qué repo/rama haciendo qué), con las sesiones abiertas de cada compañero bajo su nombre; descubrimiento de sesiones por proyecto y rol | `heartbeat`, `list_agents`, `list_sessions` |
| Ventanas autenticadas: una credencial que demuestra qué ventana llama, derivada de tu token de agente | `register_session`, `resume_session`, `renew_session`, `revoke_session` |
| **Conversaciones** (opt-in por equipo): hilos dirigidos con pertenencia explícita | `create_conversation`, `list_conversations`, `invite_to_conversation`, `join_conversation`, `leave_conversation`, `remove_conversation_member`, `transfer_membership`, `archive_conversation` |
| Mensajes de conversación con receipts por destinatario, y un long poll que devuelve cuentas | `send_conversation_message`, `read_conversation`, `get_conversation_message`, `ack_message`, `get_message_receipts`, `wait_for_conversation_updates` |
| El inbox duradero de referencias de cada ventana, y lo que puede significar `delivered` | `fetch_conversation_inbox`, `confirm_inbox_delivery`, `conversation_inbox_status` |
| Hilos de proyecto, y recuperación auditada de la historia de un asiento | `create_project`, `list_projects`, `grant_project_access`, `recover_conversation_history` |
| Memoria compartida del equipo (notas con historial) | `set_note`, `get_note`, `list_notes`, `search_notes`, `delete_note` |
| **Resumen de actividad** de las últimas N horas | `team_digest` |
| **Sesiones**: un token, un contexto de trabajo por ventana (una por conversación con `mcp proxy`, o por etiqueta `X-Crew-Session`) | ver [Sesiones](#sesiones-un-agente-varias-ventanas) |
| **Anuncios** que llegan a todas las sesiones estén en lo que estén | `announce` en `post_message` |
| Identidad | `whoami` |

Decisiones de diseño:

- **La identidad sale del token**, nunca de un argumento: un agente no puede
  hablar en nombre de otro.
- **Multi-equipo**: todo está aislado por `team`; un despliegue sirve para
  varios squads.
- **Stateless**: MCP Streamable HTTP sin sesiones *de transporte*. Las
  etiquetas de sesión y las ventanas autenticadas son estado de aplicación en
  Postgres, así que cualquier réplica atiende cualquier petición y el bus
  escala horizontal detrás de cualquier balanceador.
- **Locks honestos**: los claims de tareas llevan lease con TTL; si un agente
  muere, su tarea vuelve a estar disponible. `claim_next_task` usa
  `FOR UPDATE SKIP LOCKED`, así que N agentes en paralelo nunca reciben la
  misma tarea.
- Los tokens se guardan **hasheados** (SHA-256); el valor en claro solo se ve
  al emitirlos.

## Instalación

```bash
# macOS / Linux, con Homebrew
brew install joaquinbejar/tap/ai-crew-sync

# Debian / Ubuntu  (cambia amd64 por arm64 en máquinas ARM)
curl -LO https://github.com/joaquinbejar/ai-crew-sync/releases/latest/download/ai-crew-sync_amd64.deb
sudo dpkg -i ai-crew-sync_amd64.deb

# RHEL / Rocky / Fedora  (o ai-crew-sync.aarch64.rpm)
sudo rpm -i https://github.com/joaquinbejar/ai-crew-sync/releases/latest/download/ai-crew-sync.x86_64.rpm

# Desde el código, o como contenedor
cargo install ai-crew-sync
docker pull ghcr.io/joaquinbejar/ai-crew-sync:latest
```

Un solo binario es el servidor, el CLI de operador (aprovisionamiento junto a
Postgres, más `admin` para la administración remota), el cliente de consola y
el proxy MCP por conversación (`ai-crew-sync mcp proxy`) que arrancan el
plugin y las configuraciones de cliente recomendadas; así que instálalo en la
máquina de cada desarrollador además de en el servidor. El `.deb` y el `.rpm`
instalan además una unidad systemd endurecida y un fichero de entorno en
`/etc/ai-crew-sync/ai-crew-sync.env`, legible solo por root y por el grupo del
servicio (`root:ai-crew-sync`, `0640`). El servicio queda **deshabilitado**,
porque no puede funcionar hasta que `DATABASE_URL` apunte a un Postgres real:

```bash
sudo vi /etc/ai-crew-sync/ai-crew-sync.env   # DATABASE_URL, BUS_DASHBOARD_SECRET
sudo systemctl enable --now ai-crew-sync
```

Los binarios de Linux están enlazados estáticamente contra musl, así que
funcionan en cualquier distribución sea cual sea su glibc. Cada paquete se
instala y se ejecuta dentro de la distribución a la que apunta antes de que
una release lo publique.

## Arranque rápido (docker-compose)

```bash
make up      # crea la red `edge` si no existe, y después:
             # docker compose --project-directory . -f Docker/docker-compose.yml up -d --no-build
```

Todas las variables tienen default razonable; se sobrescriben por entorno o
en `./.env` (parte de `.env.example`, que documenta cada knob con su
default — pon un `POSTGRES_PASSWORD` real para cualquier cosa no local). El
`--project-directory .` es lo que hace que compose lea `./.env`; ejecuta los
comandos de compose con él desde la raíz del repositorio, o usa los targets de
`make`. `make up-dev` construye desde el checkout. **Docker Swarm** funciona
con el mismo fichero:

```bash
export POSTGRES_PASSWORD=...   # Swarm no lee ficheros .env
docker network create --driver overlay --attachable edge   # el bus siempre se une a ella, con Traefik o sin él
docker stack deploy -c Docker/docker-compose.yml crew
```

(`make deploy` hace lo mismo tras un preflight que además necesita las
variables de producción de abajo. `TRAEFIK_NETWORK` nombra un overlay
existente en lugar de `edge`.) El bus es stateless — escala réplicas de `bus`
sin más tras el routing mesh.

El servidor escucha en `0.0.0.0:8787` (`BUS_BIND`), migra la base de datos al
arrancar (`BUS_AUTO_MIGRATE`, activado por defecto) y expone:

- `POST /mcp` — el endpoint MCP. Requiere `Authorization: Bearer` con un
  token de agente (`acs_…`) o una credencial de sesión (`acss_…`, que
  `mcp proxy` envía tras registrar su ventana). Las credenciales
  administrativas `acsa_` se rechazan aquí.
- `GET /health` — para el balanceador. `503` solo cuando la base de datos
  está caída; si no, `200` con `status` `ok`, o `degraded` cuando el listener
  de eventos ha dejado de oírse a sí mismo (`events.listener`). Con un broker
  configurado añade `broker` (`configured`; `/health?broker=check` lo sondea
  de verdad) y el backlog del outbox en `publication`.
- `GET /dashboard` — panel read-only para humanos (presencia, tareas, locks,
  últimos mensajes de canal y notas actualizadas hace poco; los DMs y las
  conversaciones nunca aparecen). Se refresca solo cada 15s. Ábrelo en el
  navegador y pega un token de agente una vez: `POST /dashboard/login` lo
  intercambia por una cookie de sesión HttpOnly, de vida corta y de solo
  lectura, que **no puede llamar a herramientas MCP**. Los scripts se saltan
  el intercambio y mandan `Authorization: Bearer acs_...` directamente. El
  token nunca se acepta en la query string — una URL acaba en el historial,
  en los referrers y en los logs del proxy.
- `/admin/*` — la API de administración, solo para credenciales
  administrativas `acsa_`; los comandos `ai-crew-sync admin` hablan con ella
  (ver [Administración remota](#administración-remota)).

Siguiente: [crear la primera credencial administrativa](#dar-de-alta-al-equipo),
el único paso que se ejecuta junto a Postgres.

### Desplegar en producción

Hay un único fichero compose, y trae un default que funciona para todo, para
que `make up` arranque en un portátil. Lo que protege producción es el
preflight que `make deploy` ejecuta antes de tocar el clúster:

```bash
export POSTGRES_PASSWORD=…        # no el valor de ejemplo
export BUS_VERSION=0.7.0          # una versión de release completa, nunca `latest`
export BUS_ALLOWED_HOSTS=crew.example.com
export BUS_DASHBOARD_SECRET=…     # compartido, para que la sesión valga en cualquier réplica
make deploy                       # preflight y después docker stack deploy
```

`make deploy` se niega si falta alguno, si la contraseña sigue siendo
`change-me`, si `BUS_VERSION` es `latest`, o si se pone `TRAEFIK_ENABLE=true`
sin `BUS_PUBLIC_HOST`; también se niega cuando la red del proxy
(`TRAEFIK_NETWORK`, por defecto `edge`) no existe en el swarm. `make
deploy-check` ejecuta solo las comprobaciones de variables y nunca contacta
con el clúster. Fija una versión completa como `0.7.0`: cada release publica
además un tag móvil `0.7`, y el preflight no lo detecta.

Detrás de un proxy Traefik v3 (`--providers.swarm`) que ya termina TLS, pon
`TRAEFIK_ENABLE=true` y `BUS_PUBLIC_HOST=crew.example.com` (más
`TRAEFIK_NETWORK`/`TRAEFIK_ENTRYPOINT`/`TRAEFIK_CERTRESOLVER` si difieren de
`edge`/`websecure`/`le`): el bus lleva las labels del router. Se une a la red
del proxy en todos los despliegues, con Traefik o sin él (solo las labels
dependen de `TRAEFIK_ENABLE`), y el stack del proxy debe haber creado esa red
como overlay attachable. Traefik reenvía la cabecera `Host` original, así que
`BUS_ALLOWED_HOSTS` debe incluir `BUS_PUBLIC_HOST` (o ser `*` cuando el proxy
ya valida `Host`); si no, toda llamada a `/mcp` que pase por él se rechaza con
`403`.

## Dar de alta al equipo

La administración se hace desde tu propia máquina con una **credencial
administrativa** (prefijo `acsa_`). Solo la primera hay que emitirla junto a
Postgres, porque todavía no existe nada que pueda autorizar una petición
remota. Tres pasos, una vez por despliegue:

### 1. Bootstrap: la primera credencial administrativa

`admin bootstrap` necesita `DATABASE_URL` y una base de datos migrada: el
servidor migra al arrancar; si no, ejecuta antes `ai-crew-sync migrate`. No
hace falta que exista ningún equipo todavía. Ejecútalo donde tengas
`DATABASE_URL` a mano:

```bash
# Stack de Compose, desde la raíz del repositorio: el contenedor del bus tiene DATABASE_URL
docker compose --project-directory . -f Docker/docker-compose.yml exec bus \
    ai-crew-sync admin bootstrap --label "portátil de joaquin"

# Docker Swarm (stack `crew`), en un nodo que ejecute una réplica del bus
docker exec -it $(docker ps -q -f name=crew_bus | head -n1) \
    ai-crew-sync admin bootstrap --label "portátil de joaquin"

# .deb / .rpm: DATABASE_URL vive en el fichero de entorno del servicio
sudo sh -c 'set -a; . /etc/ai-crew-sync/ai-crew-sync.env; exec ai-crew-sync admin bootstrap --label "portátil de joaquin"'

# Cualquier otro sitio que llegue a la base de datos
DATABASE_URL=postgres://bus:…@db-host:5432/bus ai-crew-sync admin bootstrap --label "portátil de joaquin"
```

```text
Global administrative credential — shown once, store it now:

  acsa_3f9c…

Use it from your machine with `ai-crew-sync admin login --url <bus>`.
1 global credential(s) are now active; list them with `admin credential list`.
```

El secreto se imprime una sola vez; el bus guarda solo su SHA-256.
`bootstrap` nunca se niega porque ya exista una credencial (cada ejecución
emite una credencial global más), y eso lo convierte también en la forma de
volver a entrar cuando se pierde la última (ver
[Recuperación](#recuperación-se-ha-perdido-la-última-credencial-global)). El
label es una nota para humanos.

Una credencial administrativa es una clase distinta del token de agente
(`acs_`): no identifica a nadie en el bus, no puede publicar, reclamar ni leer
nada, y solo gestiona equipos, agentes, tokens de agente y credenciales
administrativas. Los tokens de agente, a su vez, nunca pueden emitir
credenciales administrativas ni otros tokens de agente, ni siquiera para su
propio agente: lo único que derivan es una credencial de sesión para una de
sus propias ventanas (`register_session`). Cada emisión, concesión y
revocación queda en una tabla de auditoría que jamás contiene un secreto.

### 2. Iniciar sesión desde tu máquina

```bash
ai-crew-sync admin login --url https://crew.example.com   # pide el secreto acsa_… (sin eco)
ai-crew-sync admin whoami                                 # qué credencial, y qué puede administrar
```

`login` comprueba la credencial contra el bus antes de guardarla (permisos
`0600`, en el directorio de configuración). A partir de aquí nada necesita
SSH, `docker exec` ni conexión a la base de datos.

### 3. Crear el equipo, sus agentes y sus tokens

```bash
ai-crew-sync admin team add --slug acme --name "Acme Squad"
ai-crew-sync admin agent add --team acme --name joaquin
ai-crew-sync admin agent add --team acme --name marta
ai-crew-sync admin token issue --team acme --agent joaquin --label "portátil de joaquin"   # se imprime una vez
ai-crew-sync admin token issue --team acme --agent marta   --label "portátil de marta"
```

Cada token se enseña **una sola vez**, y solo después de que el bus haya
confirmado que autentica exactamente como ese agente. Dos formas de no ir
pasando tokens de mano en mano: `admin token issue … --save --repo <entrada>`
lo escribe directamente en `tokens-<equipo>` en la máquina que lo ejecuta y
no lo imprime nunca, y `admin grant --team acme` le da a un compañero una
credencial administrativa limitada a ese equipo, para que emita los suyos.
[Administración remota](#administración-remota) tiene el juego completo de
comandos.

**Sin la API remota.** Junto a Postgres, lo mismo se puede hacer con los
comandos locales de operador, que necesitan `DATABASE_URL` y ninguna
credencial:

```bash
ai-crew-sync team create --slug acme --name "Acme Squad"
ai-crew-sync agent add --team acme --name joaquin     # siempre emite un token y lo imprime una vez
ai-crew-sync agent add --team acme --name marta
```

Gestión posterior: `team list`, `agent list`, `agent disable` (volver a
ejecutar `agent add` rehabilita un agente deshabilitado, con un token nuevo),
`token issue`, `token list`, `token revoke`. Todos salvo `agent disable`
existen también en remoto como `admin team|agent|token …`. Todos los comandos
están en la [referencia de la línea de
comandos](#referencia-de-la-línea-de-comandos).

### Un agente por herramienta, no por persona

Si usas Claude Code *y* Codex —o dos agentes de código cualesquiera— dale a
cada uno su propio agente:

```bash
ai-crew-sync admin agent add --team acme --name joaquin         # Claude Code
ai-crew-sync admin agent add --team acme --name joaquin-codex   # Codex
```

Compartir un token entre dos herramientas las convierte en **el mismo agente**
para el bus. A través de `mcp proxy` (o con etiquetas `X-Crew-Session`
distintas) cada ventana sigue teniendo su propia sesión, así que claims,
locks, presencia y cursores de lectura se mantienen separados, y el claim de
una ventana hermana se rechaza ("claimed by your own … session"). Lo que
pierdes es la identidad: las dos herramientas son un solo nombre en `whoami`,
`list_agents` y `team_digest`, un mensaje directo a `joaquin` a secas llega a
las dos, revocar o deshabilitar una corta a las dos, y tus compañeros no
pueden saber qué herramienta contestó. Dos clientes que no envían ninguna
sesión comparten una, y entonces la coordinación deja de funcionar entre ellos
en silencio: los dos reclaman la misma tarea y a los dos se les dice que la
tienen, uno suelta el lock del otro, y leer en uno marca como leídos los
mensajes del otro.

Con agentes separados todo eso funciona como debe, revocarle el acceso a una
herramienta no toca la otra, y además pueden hablarse: `ask_agent` de Claude
Code a `joaquin-codex` se comporta igual que preguntarle a un compañero.

**Los conflictos de fichero no son problema del bus.** Dos agentes editando
los mismos ficheros a la vez se pelearán diga lo que diga el bus. Los claims y
los locks son la herramienta para eso, y alguien tiene que usarlos — o darle a
cada agente su rama o su worktree.

### Canales, claves de tarea y nombres de lock

Un canal por repo más uno para el equipo:

```
create_channel market-data
create_channel core-manager
create_channel general
```

Un canal se convierte en el canal por defecto de una ventana cuando se llama
como el proyecto de esa ventana (`context set-project`, que por defecto toma
el nombre del directorio) o cuando se nombra directamente con `--channel` /
`configure_session`; ver [El canal de la sesión](#el-canal-de-la-sesión) más
abajo.

Dos convenciones que importan en cuanto un equipo tiene más de un repo:

- **Las claves de tarea son únicas por equipo, no por repo.** `issue-151`
  colisiona en cuanto dos repos tienen una; ponles prefijo: `market-data#42`,
  `core-manager#151`.
- **Los nombres de lock también son de todo el equipo.** Un `deploy` cogido
  para un repo bloquea el despliegue del otro; usa `market-data:deploy`.

## Conectar cada agente

Vale cualquier cliente MCP: el bus es Streamable HTTP estándar con token
Bearer. Claude Code tiene plugin listo (opción A); cualquier otro agente —
Codex, Cursor, Kimi, Zed, un script — usa la configuración MCP estándar de la
opción B.

### Opción A (Claude Code): plugin

Este repo es también un *marketplace* de plugins de Claude Code. Cada compañero
instala el binario una vez (Homebrew, `.deb`/`.rpm` o `cargo install`, ver
**Instalación**), le dice en qué bus está, y después instala el plugin dentro
de Claude Code:

```bash
# --url es la URL base del bus; el /mcp se añade solo
ai-crew-sync context profile add --name acme --default \
    --url https://crew.example.com --team acme --agent joaquin
ai-crew-sync context show            # endpoint, perfil, identidad esperada, prefijo del token
ai-crew-sync context verify          # le pregunta al bus quién es DE VERDAD ese token
```

El perfil lee sus credenciales de `tokens-acme` en el directorio de
configuración del propio compañero (`~/.config/ai-crew-sync` por defecto). O
bien lo rellena el compañero en su máquina: con una credencial administrativa
de equipo (`admin grant --team acme`), `admin token issue --team acme --agent
joaquin --save --repo <entrada>` escribe ahí el token y nunca lo imprime; o
bien un administrador lo emite sin `--save` y le pasa el token impreso.
`<entrada>` es el nombre del proyecto (`context set-project`, por defecto el
nombre del directorio), un `key` de `.acs.toml` o del perfil, o `_base`.

```
/plugin marketplace add joaquinbejar/ai-crew-sync     # o tu fork
/plugin install ai-crew-sync@ai-crew-sync
```

**El plugin necesita `ai-crew-sync` en el PATH**, y sus hooks necesitan además
`python3` (sin él, las herramientas MCP y la presencia siguen funcionando; el
resumen de inicio de sesión y el drenaje de preguntas no hacen nada, en
silencio). Su entrada MCP no es una URL
HTTP con un token dentro: arranca `ai-crew-sync mcp proxy`, un proceso por
conversación, y ese proceso resuelve una credencial de tus perfiles locales y
registra la ventana como *sesión autenticada*. Ni la configuración del plugin
ni tu shell tienen que guardar un token. Quien prefiera el par de siempre
puede seguir: `mcp proxy` también lee `BUS_URL` y `BUS_TOKEN`. Ojo a la
precedencia, que es la del resolver y no la del plugin: un `BUS_TOKEN`
exportado (igual que `--token`) **gana** al perfil del `.acs.toml` del
repositorio y al perfil por defecto, y junto con `--profile` es un error, no
una elección silenciosa. Migrar desde una función de shell que exporta el
token por directorio significa dejar de exportarlo (o retirar la función)
allí donde deba decidir el perfil; mientras esté exportado, el perfil nunca
aplica.

El plugin trae todo preconfigurado:

- **MCP** `ai-crew-sync` a través del proxy por conversación (sin tocar JSON a
  mano y sin credencial en la configuración). Cada ventana de Claude Code pasa
  a ser su propia dirección `agent/session`, probada con una credencial
  propia; dos ventanas en el mismo repo ya no se pueden confundir.
- **Hooks**: al arrancar una sesión hace heartbeat y le inyecta a Claude un
  resumen del equipo (DMs sin leer, tareas propias, `team_digest` de las últimas
  8 h — configurable con `BUS_DIGEST_HOURS`); tras cada respuesta renueva la
  presencia con el repo/rama del checkout, y al cerrar sesión marca `idle`.
  Sin ningún bus configurado, no hacen nada.

Para esto ya no hace falta `BUS_SESSION`: el proxy deriva la sesión del id de
conversación que le da Claude Code, así que una ventana mantiene su identidad
al reconectar, un fork estrena una nueva, y presencia, claims y locks se
separan solos. Lo que sí merece la pena poner por repo es el *proyecto* y el
*rol*, para que los compañeros encuentren la ventana correcta:

```bash
ai-crew-sync context set-project --profile acme --project market-data --channel market-data
```

Ejecútalo desde la raíz del repositorio (o pasa `--dir`). Escribe
`.acs.toml`: el nombre del perfil, la etiqueta de proyecto (por defecto el
nombre del directorio; también la entrada de `tokens-<equipo>` de la que se lee
la credencial, salvo que `--key` nombre otra) y el canal por defecto; jamás
una URL ni una credencial, así que se puede commitear. Con el plugin, el rol
de la ventana se fija en caliente con la herramienta local `configure_session`
del proxy; `ai-crew-sync proxy-config --role review` es para una configuración
de cliente escrita a mano (opción B).

El hook `Stop` además drena preguntas: cuando el agente de un compañero está
bloqueado en `ask_agent`, la sesión se mantiene abierta lo justo para
contestar antes de callarse — primero la pregunta que lleva más tiempo
esperando, una por turno, y nunca una que ya hayas respondido. **Esto no hace contestable
una sesión parada**: un agente de código solo llama a herramientas mientras
procesa un turno, así que una ventana parada una hora en el prompt no contesta
hasta que su humano escriba. Es una propiedad del cliente, no del bus; para lo
que no pueda esperar, usa una tarea o un mensaje de canal.

- **Comandos**: `/ai-crew-sync:standup [horas]`, `/ai-crew-sync:catchup [horas]`,
  `/ai-crew-sync:announce [#canal] mensaje`, `/ai-crew-sync:ask <agente> <pregunta>`,
  `/ai-crew-sync:claim <clave|next>`, `/ai-crew-sync:done <clave> [resultado]`,
  `/ai-crew-sync:handoff <clave> <agente[/sesión]>`, `/ai-crew-sync:board`,
  `/ai-crew-sync:who`, `/ai-crew-sync:lock <recurso>`, `/ai-crew-sync:unlock [recurso]`,
  `/ai-crew-sync:note <clave> [texto]`, `/ai-crew-sync:wait [tipos]`,
  `/ai-crew-sync:thread <direcciones> -- <título>`, `/ai-crew-sync:inbox` y
  `/ai-crew-sync:review <agente> <PR>`. Cada uno es un procedimiento escrito
  sobre los tools del bus: las mismas tres o cuatro llamadas en el mismo orden,
  con las mismas salvaguardas, desde cualquier ventana. `thread` e `inbox`
  necesitan la capacidad de conversaciones del equipo (`ai-crew-sync team
  capability --team <equipo> --conversations on`, junto a Postgres).
- **Skill** con las convenciones (reclamar antes de trabajar, locks para
  deploys, `wait_for_updates` para esperar respuestas), que Claude carga solo
  cuando toca coordinarse.

#### Los mismos procedimientos para cualquier otro host

Los comandos se generan desde `recipes/`, un Markdown por procedimiento
escrito para un agente que tiene los tools del bus y nada más: sin
frontmatter, sin supuestos de host, con `{{input}}` donde van los argumentos.
Para Codex, añade una línea al `AGENTS.md` de tu propio repositorio que apunte
a ellos (`ai-crew-sync recipes <nombre>`; `examples/CLAUDE.md-snippet.md` trae
la redacción lista). A Kimi Code, Grok o cualquier cliente MCP se le
pega uno tal cual, y una máquina con el binario pero sin el repositorio los
obtiene con `ai-crew-sync recipes` (lista) y `ai-crew-sync recipes catchup`
(uno). `make recipes` regenera los slash commands; `make check` y los tests
unitarios fallan si un comando se aparta de su receta, así que cada
procedimiento se edita en un solo sitio.

Los hooks eligen una de tres vías por conversación, probadas en este orden:

- **Autenticado** — la ventana tiene proxy, así que los hooks llaman a
  `ai-crew-sync context hook`, que lee el binding privado de esa ventana y
  actúa como esa sesión. La credencial no pasa por ningún script, argumento ni
  variable de entorno. Necesita el binario en el PATH, que el plugin ya exige.
  Una ventana con binding sigue siéndolo diga lo que diga el entorno: un
  `BUS_TOKEN`/`BUS_SESSION` exportado no desvía su drenaje de Stop al inbox de
  otra sesión. El binario sirve a los hooks solo las cuatro herramientas que
  usan sus scripts (`whoami`, `read_messages`, `team_digest`, `heartbeat`),
  así que un hook nunca puede emitir, rotar ni revocar una credencial. Solo
  una conversación sin binding alguno toma las vías de abajo.
- **Perfiles locales** — no hay binding utilizable ni `BUS_TOKEN` exportado,
  pero el binario está en el PATH. Los hooks ejecutan
  `ai-crew-sync client --json call` con credenciales de `profiles.toml` y
  `.acs.toml`, y la misma etiqueta de sesión que el proxy de la conversación
  deriva de su id (una etiqueta, no una prueba).
- **Legacy** — `BUS_URL`/`BUS_TOKEN` exportados. Los hooks caen a `curl` +
  `python3` pelados y a la etiqueta `X-Crew-Session`, exactamente como antes.
  No se asume nada más instalado.

Ninguna de las tres configurada: los hooks no hacen nada, en silencio.

### Opción B (cualquier cliente MCP): configuración manual

Dos formas, y la diferencia es qué prueba qué ventana está llamando.

**Por el proxy (recomendado).** El cliente arranca el binario en vez de abrir
una conexión HTTP; cada conversación tiene su propia sesión autenticada, y la
configuración del cliente no guarda ninguna credencial: el proxy resuelve el
token del agente desde tu perfil local (o `BUS_TOKEN`/`BUS_URL`) y guarda su
credencial de sesión en un fichero privado `0600`. Genera el
bloque:

```bash
ai-crew-sync proxy-config                       # forma .mcp.json (la mayoría de clientes)
ai-crew-sync proxy-config --format toml         # ~/.codex/config.toml (Codex)
ai-crew-sync proxy-config --role review         # las ventanas de aquí arrancan como revisoras
```

`proxy-config` también acepta `--project` y `--profile`, y el propio
`mcp proxy` acepta `--project`, `--role`, `--channel` y `--profile` (las
etiquetas y el perfil con los que arranca la ventana, por encima de
`.acs.toml` y del valor por defecto del usuario) más `--project-dir` y
`--host-session`: los equivalentes de arranque de `configure_session`.

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "command": "ai-crew-sync",
      "args": ["mcp", "proxy"]
    }
  }
}
```

Listos para copiar: `examples/.mcp.json` y `examples/codex-config.toml`.

**HTTP directo.** No necesita binario, y el bus es Streamable HTTP pelado con
un Bearer token — es lo que usa un script, un job de CI o un cliente MCP al
que no puedes darle un comando. La identidad es el token; la ventana es una
*etiqueta* `X-Crew-Session`, en la que el bus confía para separar presencia y
claims, pero nunca como prueba de quién eres:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://crew.example.com/mcp",
      "headers": {
        "Authorization": "Bearer ${TEAM_BUS_TOKEN}",
        "X-Crew-Session": "market-data"
      }
    }
  }
}
```

Listo para copiar: `examples/.mcp.http.json`. El bloque también se genera:

```bash
ai-crew-sync mcp-config --url https://crew.example.com/mcp \
    --token acs_... --session market-data
```

`mcp-config` escribe el token en el bloque tal cual (`Bearer acs_…`): deja el
fichero generado fuera del control de versiones, o sustituye el valor por una
referencia a una variable de entorno como `${TEAM_BUS_TOKEN}`, igual que
arriba. Sin `--url` apunta a `http://localhost:8787/mcp`.

Con eso, cada agente ve las herramientas del bus y las usa solo. Para que las
use *bien*, añade las convenciones del equipo al fichero de instrucciones del
repo (`CLAUDE.md`, `AGENTS.md` o equivalente) — hay un snippet listo en
`examples/CLAUDE.md-snippet.md`.

### Perfiles locales y valores por proyecto (sin exportar `BUS_TOKEN`)

El cliente de consola (y todo lo que se apoya en él) puede encontrar sus
credenciales sin exportar nada en cada shell ni usar una función envoltorio.
Lo hacen dos ficheros locales:

- **Perfiles** — `profiles.toml` en el directorio de configuración: qué bus,
  equipo y agente esperados, y *qué fichero de tokens* guarda la credencial
  (los mismos `tokens-<equipo>` que escribe `admin token issue --save`). El
  perfil no contiene ningún secreto.
- **Valores del proyecto** — `.acs.toml` en la raíz del proyecto, commiteado
  con el código: nombra un perfil aprobado, el proyecto lógico y,
  opcionalmente, un `channel` por defecto y un `key` de fichero de tokens.
  Nada más: cualquier otra clave se rechaza, y `url`, `endpoint`, `token`,
  `tokens`, `bearer` o `secret` se rechazan con un error explícito.

El directorio de configuración es `$BUS_CONFIG_DIR` si está definido; si no,
`$XDG_CONFIG_HOME/ai-crew-sync`; y si no, `~/.config/ai-crew-sync`. Contiene
`profiles.toml`, los ficheros `tokens-<equipo>`, el fichero de login `admin` y
`sessions/` (`0700`), donde cada proxy guarda el binding y la credencial de
sesión de su conversación en un fichero `0600` cuyo nombre es un SHA-256 del id
de conversación.

```bash
ai-crew-sync context profile add --name acme --url https://crew.example.com \
    --team acme --agent joaquin --tokens tokens-acme --default
ai-crew-sync context profile list
ai-crew-sync context profile default acme      # o: --clear
ai-crew-sync context profile remove old-bus
cd ~/Repos/acme/market-data
ai-crew-sync context set-project --profile acme --project market-data --channel market-data
ai-crew-sync context show      # endpoint, perfil, entrada del token (solo prefijo), proyecto
ai-crew-sync context verify    # pregunta al bus: debe ser joaquin@acme, o falla
ai-crew-sync client whoami     # sin BUS_TOKEN
```

La entrada del token se elige en este orden: `key` de `.acs.toml`
(`set-project --key`), el nombre del proyecto, el `key` del perfil
(`profile add --key`) y por último `_base`; `--tokens` vale por defecto
`tokens-<equipo>`. La precedencia entre fuentes es fija, y `context show`
imprime cuál ganó (`explicit`, `profile-flag`, `project-default` o
`user-default`):

| Orden | Fuente | Notas |
|---|---|---|
| 1 | `--token` / `BUS_TOKEN` (+ `--url` / `BUS_URL`) | Las credenciales explícitas siempre ganan; `.acs.toml` sigue aportando proyecto y canal. Junto con `--profile` es un error, no una elección silenciosa. Sin URL se conectan a `http://localhost:8787/mcp`. |
| 2 | `--profile` / `BUS_PROFILE` | Elección por invocación; nunca reescribe los valores del proyecto. Cuando se da un id de conversación del host (`--host-session` / `BUS_HOST_SESSION`, como hacen los hooks), el perfil en el que se quedó el proxy de esa conversación cuenta como esta elección. |
| 3 | `.acs.toml` en la raíz del proyecto | Se encuentra desde cualquier subdirectorio; un worktree enlazado hereda el fichero del worktree principal. |
| 4 | `default = "…"` en `profiles.toml` | Valor por defecto del usuario. |

`--url` / `BUS_URL` sustituye el endpoint sea cual sea la fuente que elige la
credencial, perfil incluido: un `BUS_URL` exportado que se quedó olvidado
manda el token del perfil a esa URL, así que quítalo al pasarte a perfiles.

La precedencia no se mueve, pero ya no es invisible: cuando un `BUS_TOKEN` o
`BUS_URL` exportado y olvidado le gana a un perfil instalado, cada comando
imprime un aviso por stderr nombrando el perfil eclipsado (el proxy lo manda
al log, manteniendo limpio el stdout del protocolo MCP). `context show` y
`context verify` informan de dónde salió cada cosa — `BUS_TOKEN (environment)`,
`--token (flag)`, o la entrada del fichero de tokens y el perfil que la
seleccionó — y `verify` repite esa procedencia cuando el bus rechaza el token,
de modo que el error nombra la fuente y no solo el síntoma. La credencial en
sí nunca se imprime. Desde 0.7.1 el `serverInfo` del handshake lleva
`ai-crew-sync` y la versión real del servidor (los servidores anteriores
informan la del framework), y el proxy registra una pista de desalineación de
versiones cuando su binario y el bus difieren: una pista para alinearlos,
nunca un rechazo.

Un perfil que no existe localmente es un **error**, lo nombre quien lo nombre:
un repositorio puede sugerir un perfil, nunca definirlo, y `.acs.toml` se
rechaza de plano si trae `url`, `token` o una ruta de tokens. Endpoints y
referencias a credenciales salen solo de tu propio almacén de perfiles, así que
un repositorio clonado no puede enviar tu token a ningún sitio. Dos ventanas en
el mismo repositorio eligen perfil de forma independiente (`--profile`) sin
compartir nada mutable. Las escrituras al almacén de perfiles se serializan
con un lock y aterrizan de forma atómica con permisos `0600`.

### Sesiones autenticadas: demostrar qué ventana eres

`X-Crew-Session` es una etiqueta que elige quien llama. Basta para que las
ventanas de una persona no se pisen presencia y claims, y no demuestra nada:
quien tenga el token de agente puede mandar cualquier etiqueta.

`register_session` convierte una ventana en algo demostrable. Presentas tu
token de agente una vez por conversación y el bus devuelve una credencial
derivada de él:

```
register_session {"session": "conv-7f2a"}
→ {"session_token": "acss_…", "session_id": "…", "session": "conv-7f2a",
   "address": "joaquin/conv-7f2a", "epoch": 1,
   "expires_at": "…", "expires_in_seconds": 86400}
```

A partir de ahí la envías como bearer token. Autentica como tu agente, en esa
única sesión, y:

- **todo lo hereda del padre**: agente y equipo salen del token que la
  registró, así que un cliente no puede afirmar ninguno de los dos;
- **no emite nada**: ni tokens de agente, ni credenciales administrativas, ni
  otra sesión;
- **caduca sola** (24 horas por defecto, `ttl_seconds` para menos) y **muere
  con su padre**: revoca el token o deshabilita el agente y todas sus sesiones
  dejan de autenticar, sin barrido que esperar;
- **rechaza una cabecera que la contradiga**: una petición cuyo
  `X-Crew-Session` nombre otra ventana se rechaza, así que una sesión probada
  nunca se amplía a la de otro;
- **no se entrega a quien tenga el token del agente**: registrar una
  etiqueta viva se *rechaza* (un error de tool que empieza por
  `conflict: session '…' is already registered and still live`); la única
  vuelta a una ventana es su propia credencial, vía `resume_session`, que
  emite secreto nuevo, incrementa `epoch` y deja identidad e historia
  intactas. Una etiqueta cuya credencial venció o fue revocada sí puede
  registrarse otra vez, como ventana nueva con el nombre antiguo;
- **vence a lo que sustituye**: manda el epoch en `X-Crew-Epoch` y un proceso
  al que han reemplazado se entera (`409`) en vez de escribir como la ventana
  que lo sustituyó.

`renew_session` alarga la credencial que ya tienes sin tocar su secreto ni su
epoch. `revoke_session` la cierra, o cierra otra ventana de tu propio agente
por etiqueta. `whoami` incluye `session_identity` (id de sesión, epoch,
registro y caducidad) cuando la etiqueta está probada por una credencial de
sesión, y omite el campo cuando la etiqueta solo es una cabecera.

Nada de esto es obligatorio: un token de agente con cabecera de sesión sigue
funcionando en todos los tools y con un `curl` suelto, igual que antes.

### Una sesión por conversación: el proxy stdio

Las sesiones separan ventanas, pero una cabecera escrita una vez en la
configuración del cliente es la misma en todas sus ventanas.
`ai-crew-sync mcp proxy` es un servidor MCP local que el cliente arranca **una
vez por conversación** (la norma para servidores stdio), así que el proceso es
la unidad de aislamiento: acuña la sesión, la manda en cada llamada reenviada
y guarda el proyecto y el rol de esa ventana. Funciona con cualquier cliente
MCP; lo que ofrezca un host concreto se aprovecha, nunca se exige.

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "command": "ai-crew-sync",
      "args": ["mcp", "proxy", "--role", "implementation"]
    }
  }
}
```

Sin token ni URL en la configuración del cliente: las credenciales salen de tu
perfil local y del `.acs.toml` del proyecto (más arriba). Aparecen todos los
tools remotos y dos que nunca llegan al bus:

- `session_status` — agente y equipo verificados, id de sesión, **dirección**
  (`agente/sesión`), proyecto, rol y canal. Nunca credenciales.
- `configure_session({role?, project?, channel?, profile?})` — solo esta
  ventana. Rol y proyecto son metadatos: el id de sesión, los cursores, los
  claims y los locks no se tocan. `profile` cambia a otra credencial aprobada
  localmente **del mismo equipo**, verificada con `whoami` antes de cambiar
  nada; si la verificación falla el contexto anterior sigue intacto, las
  llamadas en vuelo de la identidad anterior se cancelan en vez de
  reintentarse, y lo que esa identidad aún sostiene (claims, locks) se informa,
  nunca se transfiere. Otro equipo exige una conversación nueva: cambiar de
  credencial no puede borrar lo que esta conversación ya ha visto.

El proxy hace todo esto por ti: registra la sesión al conectar, reenvía cada
llamada con la credencial y el epoch, renueva la credencial a mitad de su
vida tanto si la ventana está ocupada como ociosa (`BUS_SESSION_TTL_SECS` y
`BUS_SESSION_RENEW_LEAD_SECS` ajustan la vida que pide y la antelación), y
al cerrarse la ventana marca su binding como cerrado. La credencial
**se queda** en ese fichero 0600: es la única forma de reanudar la misma
ventana tras reiniciar la conversación, porque el bus se niega a entregar una
sesión viva al token del agente. Un binding cerrado no lo usan los hooks, y la
credencial conserva solo la vida que el bus le dio; `revoke_session` la
termina antes. Una renovación que el bus rechace la informa `session_status`
como credencial rechazada; nunca se tapa con otra identidad. Un bus demasiado antiguo para
emitir credenciales se queda simplemente con la conexión por etiqueta.

**Identidad de la conversación**, por orden: `--host-session` /
`BUS_HOST_SESSION` (cualquier host que pueda fijar una variable por ventana),
`CLAUDE_CODE_SESSION_ID` (Claude Code la exporta a los procesos MCP), el
`_meta.threadId` que Codex adjunta a cada llamada, y si no el propio proceso.
Con un id de conversación la etiqueta de sesión es **estable**: una
conversación reanudada vuelve a la misma sesión y una bifurcada recibe otra;
sin él, la sesión dura lo que el proceso. Cuando la conversación se identificó
por el `_meta` de la petición (`threadId`, o `sessionId`), un segundo id de
conversación en el mismo proceso se rechaza en vez de mezclarse. Cuando la
fijó `--host-session` / `BUS_HOST_SESSION` o `CLAUDE_CODE_SESSION_ID`, ese
binding manda y los ids que llegan en las peticiones se ignoran, así que
configura el host para que arranque un proxy por conversación.

El campo `instructions` del resultado de `initialize` le dice al modelo quién
es y dónde está esta ventana (agente, equipo, dirección de sesión, proyecto,
rol, canal), más las convenciones del propio bus; todo cliente MCP lo entrega,
así que esto no necesita hooks. Los mensajes directos sin leer, los claims
abiertos y el digest del equipo los añade un hook de inicio de sesión donde el
host lo tiene; en los demás, el modelo llama él mismo a `whoami` y
`team_digest`. Donde el host **sí** tiene hooks de ciclo de vida, estos
ejecutan
`ai-crew-sync context hook --binding <id de conversación> --event <evento>`:
el helper lee el registro privado que escribió el proxy (directorio 0700,
fichero 0600), actúa como **esa** ventana con su propia credencial e imprime
solo lo que el host espera. La credencial nunca pasa por argv, stdout ni
logs, un hook nunca registra (así que no puede vencer a su propio proxy), y
una conversación sin binding no imprime nada en vez de actuar como una
identidad compartida. Igual que el proxy, este modo autenticado necesita el
binario `ai-crew-sync` en el PATH; el modo legacy sigue necesitando solo
`curl` y `python3`.

La presencia la mantiene el propio proxy: heartbeat al conectar con repo y
rama del directorio del proyecto, keep-alive cada cinco minutos e `idle` al
salir. Nada se empuja
a un turno inactivo: los mensajes entrantes se leen con `read_messages` o se
esperan con `wait_for_updates`, igual que con una conexión directa.

### Sesiones: un agente, varias ventanas

Un token identifica a un **agente** (uno por herramienta de código y
persona), y un agente suele tener varias ventanas abiertas a la vez. Con el
plugin o con `mcp proxy`, cada conversación ya tiene su propia sesión (una
etiqueta opaca `s-…`, ver
[Una sesión por conversación](#una-sesión-por-conversación-el-proxy-stdio))
y no hay nada que configurar. Un cliente HTTP directo que no puede arrancar
el binario elige su sesión a mano con la cabecera `X-Crew-Session`:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://crew.example.com/mcp",
      "headers": {
        "Authorization": "Bearer ${TEAM_BUS_TOKEN}",
        "X-Crew-Session": "${BUS_SESSION:-}"
      }
    }
  }
}
```

La etiqueta tiene hasta 64 bytes de ASCII, sin `/` (separa agente de sesión en
una dirección), sin caracteres de control y sin `$`, `{` ni `}`: un
`${BUS_SESSION}` literal se rechaza, así que usa `${BUS_SESSION:-}` y una
variable sin definir no envía nada. Se normaliza como un nombre de canal (sin
espacios sobrantes y en minúsculas, para que `Market-Data` y `market-data`
sean una sola sesión y no dos que no se ven entre sí). El nombre del repo es
la elección natural.

Una sesión **no** es identidad. Llega en una cabecera y no en el token, así
que nunca puede hacerte hablar por otro; solo separa tu presencia, tus claims
y tus locks de tus otras sesiones. Si omites la cabecera tienes la sesión
compartida, que es exactamente como se comportaba el bus antes de que las
sesiones existieran.

El cliente de consola acepta `--session` (o `BUS_SESSION`), y
`ai-crew-sync mcp-config --url https://crew.example.com/mcp --token acs_…
--session market-data` mete la cabecera en un bloque de HTTP directo (token
incluido); para una sesión por conversación, usa `proxy-config` en su lugar.

`list_agents` pasa a dar una entrada por sesión abierta bajo el nombre de cada
compañero, así que el tablero dice quién está en qué repo en vez de enseñar un
contexto que cambia cada vez que otra sesión manda un heartbeat:

```
joaquin
  /market-data      active  Layer-V/market-data@devops/scanning  ejecutando la suite
  /core-manager     idle    Layer-V/core-manager@issue-151
dani                active  Layer-V/core-manager@issue-151       settlements v2
```

`online_count` cuenta *compañeros*, no sesiones. Una sesión que deja de mandar
heartbeat caduca sola y no toca a las demás.

#### Encontrar la ventana correcta: `project` y `role`

Una etiqueta de sesión separa bien claims y cursores y se teclea mal, sobre
todo cuando son ids opacos acuñados por conversación. Dos **etiquetas de
descubrimiento** opcionales en `heartbeat` lo resuelven: `project` (el
proyecto lógico, normalmente el repositorio) y `role` (qué hace esa ventana
allí: `implementation`, `design`, `review`, …). Después:

```
list_sessions {"project": "market-data", "role": "review", "online_only": true}
→ {"sessions": [
     {"agent": "joaquin", "session": "s-9c0d1e2f", "address": "joaquin/s-9c0d1e2f",
      "project": "market-data", "role": "review", "status": "active", "online": true, …},
     {"agent": "joaquin", "session": "s-3a4b5c6d", "address": "joaquin/s-3a4b5c6d", …}],
   "count": 2, "limit": 200}
```

`address` es lo que va en `to` (o en el `to` de `ask_agent`), y **`exact` dice
si llega a una sola ventana**. Es `true` para una sesión con nombre y `false`
para la compartida, cuya dirección es el nombre pelado del agente: eso llega a
*todas* sus ventanas, incluidas las nombradas, así que nunca es un destino
privado y no existe ninguna dirección que llegue solo a la sesión compartida.
Dos revisores comparten rol y conservan dos direcciones exactas: el
descubrimiento devuelve ambas y quien llama elige una; nunca se enruta nada a
"quien tenga el rol", ni se difunde una instrucción privada a todos. Las etiquetas son lo que
una sesión dijo de sí misma: no son identidad (eso es el token), no son un
permiso, y varias ventanas pueden compartirlas. También fijan el canal por
defecto: una sesión que declaró `project = "market-data"` publica en
`#market-data` cuando no nombra canal, sea cual sea su etiqueta de sesión,
siempre que tenga una: la sesión compartida (sin etiqueta) nunca recibe canal
por defecto. Una etiqueta opaca que no coincide con ningún canal no recibe
ninguno, en vez de uno sorprendente. Omite una etiqueta para conservarla, envía `""` para
borrarla. `whoami` devuelve ambas y `list_agents` las muestra bajo cada sesión.

Consola: `ai-crew-sync client sessions --project market-data --role review
--online`, y `client beat --project market-data --role design`.

Los campos de nivel superior `activity`/`repo`/`branch` resumen **una** de las
sesiones del compañero, elegida en este orden: una sesión **viva** antes que
una muerta, una **con nombre** antes que la compartida, y luego la actualizada
más recientemente.

Lo de viva primero es a propósito. Una sesión con nombre que murió hace tres
días no debería ganarle a una compartida activa ahora — así que la compartida
sí gana cuando todas las nombradas están offline. Lee `sessions` cuando las
necesites todas; `team_digest` proyecta igual.

Un **claim y un lock pertenecen a la sesión que los tomó**, no a la persona.
Tu ventana de `core-manager` no puede renovar, soltar ni robar una tarea que
tiene tu ventana de `market-data`, y el error lo dice:

```
market-data#42 is claimed by your own 'market-data' session, and the lease
expires in 240s — continue the work there, or wait for the lease to expire
and claim it here
```

Sin eso, un token moviendo dos ventanas dejaba el lease sin valor entre ellas:
las dos reclamaban la misma tarea, a las dos se les decía que la tenían, y las
dos hacían el trabajo. Un lease caducado sigue siendo robable por cualquiera,
incluida otra sesión tuya, y así se lee: la tarea vuelve a ser `open`,
`claimed_by` es null, `lease_expired` es true y `lapsed_holder` dice quién lo
dejó caducar. `list_tasks {"status": "open"}` la incluye; renovarlo se rechaza,
reclámala de nuevo.

Los DM pueden dirigirse a una **sesión**, no solo a un agente:

| `to` | Llega a |
|---|---|
| `dani` | el agente — todas las sesiones que tenga abiertas |
| `dani/api` | solo a su contexto de trabajo `api` |

Esto es lo que hace útil una sesión coordinadora. Una ventana `general` puede
pasarle contexto a la de `market-data`, que es la que tiene el repo abierto, y
`ask_agent` funciona igual — incluso entre dos sesiones tuyas:

```
ask_agent  to: "joaquin/market-data"  question: "¿está verde la suite?"
```

Responde a `from/from_session`, no solo al nombre, o la respuesta llega a la
ventana que se dé cuenta primero en vez de a la que está bloqueada esperándola.

Una pregunta de una ventana **tuya** aparece igual que la de cualquier otro:
la unidad de la que habla el bus es la ventana, no la persona.

Cada sesión tiene su propio inbox y su propio cursor de lectura, así que ponerse
al día en una ventana no marca como leídos los mensajes de otra, y
`wait_for_updates` en una no se despierta por una pregunta dirigida a otra. Nada
se te oculta: `read_messages` con `all_sessions: true` devuelve todo lo dirigido
a ti en cualquier sesión.

**Una sesión mal escrita no es un error.** Un mensaje a `joaquin/markt-data` se
acepta y se queda ahí sin leer, porque una sesión que ahora no está abierta
sigue siendo un sitio legítimo donde dejar trabajo — que es justo la gracia de
pasarle algo a una ventana que abrirás luego. La dirección usada vuelve en
`delivered_to`, así que la errata se ve en la respuesta. `list_agents` enseña
qué sesiones están vivas de verdad.

### El canal de la sesión

El canal por defecto de una sesión es el canal que se llama como su `project`
declarado y, si no, el que se llama como su etiqueta de sesión (la sesión
compartida no tiene). Sin `channel` ni `to`, `post_message` va ahí,
`team_digest` lo resume, y `wait_for_updates` deja de despertarse con el ruido
de los canales de otros repos. A través de `mcp proxy`, el ajuste `channel`
propio de una ventana (`.acs.toml`, `--channel` o `configure_session`)
sustituye el canal por defecto solo para `post_message`; `team_digest` y
`wait_for_updates` siguen enfocados en el canal por defecto del servidor, así
que pon `channel` igual al proyecto o no lo pongas. Los DM, tareas, locks y
notas te despiertan siempre — silenciarlos escondería trabajo, no ruido.
`all_channels: true` vuelve a abarcar todo el equipo en cualquiera de las dos
llamadas, y un `channel` explícito siempre manda.

Se resuelve por nombre cada vez: no hay binding que configurar ni nada que
mantener sincronizado. Un equipo que no llame a sus canales como sus repos
simplemente no tiene default y sigue diciendo dónde va cada mensaje, igual que
hoy — `whoami` informa del canal resuelto, o `null` si no hay.

`read_messages` mantiene a propósito `"all"` como scope por defecto. Reducirlo
a un canal dejaría fuera tus mensajes directos de la lectura por defecto, que
es justo por donde llegan las preguntas.

### Anuncios

Un mensaje de canal solo despierta a las sesiones enfocadas en ese canal — que
es lo que hace útil el foco, y lo que silenciaría justo el mensaje que no puede
esperar. Para esos, el flag:

```
post_message  channel: "general"  announce: true
              body: "la migración 0010 entra en 5 min, no empujéis a main"
```

Un anuncio llega a **todas las sesiones del equipo**, estén en lo que estén, y
sale también en un `team_digest` enfocado. Es un mensaje con un id en un canal
—no una copia por canal— así que las respuestas y `reply_to` siguen
funcionando.

Resérvalo para lo que de verdad bloquea a otros: despliegues, migraciones,
cambios que rompen. Un equipo al que interrumpes por todo deja de leer los
anuncios, y entonces también se pierde el que importaba. En un mensaje directo
el flag se rechaza: ese ya llega sin filtrar.

## Actualizar

El bus, el CLI y el plugin de Claude Code se mueven por separado. Nada los
coordina por ti, así que actualiza primero el servidor: es la única pieza
dueña del esquema.

**El servidor.** Las migraciones añaden al esquema en vez de reescribirlo, así
que un reinicio rodante es seguro y la release anterior normalmente puede
funcionar contra un esquema más nuevo — pero solo con las migraciones al
arrancar desactivadas. Un binario que migra solo (lo que viene por defecto,
`BUS_AUTO_MIGRATE=true`) se niega a arrancar contra una base que tiene
migraciones que no conoce ("migration N was previously applied but is missing
in the resolved migrations"). Para volver atrás, pon
`BUS_AUTO_MIGRATE=false` (en el entorno del stack, o en
`/etc/ai-crew-sync/ai-crew-sync.env` para el servicio empaquetado) antes de
arrancar la versión anterior.

```bash
export BUS_VERSION=0.7.0
make deploy                                    # Swarm; o, desde la raíz del repositorio:
docker compose --project-directory . -f Docker/docker-compose.yml pull && make up
```

El contenedor migra al arrancar, y el servicio empaquetado también — los dos
traen `BUS_AUTO_MIGRATE=true` — así que actualizar por `.deb` o `.rpm` es el
paquete más un reinicio:

```bash
sudo dpkg -i ai-crew-sync_amd64.deb            # o: sudo rpm -U ai-crew-sync.x86_64.rpm
sudo systemctl restart ai-crew-sync
```

Si lo has desactivado y migras a propósito, carga tú el fichero de entorno:
`DATABASE_URL` vive en `/etc/ai-crew-sync/ai-crew-sync.env`
(`root:ai-crew-sync`, `0640`), que systemd carga para la unidad y que ninguna
shell carga por ti.

```bash
sudo systemctl stop ai-crew-sync
sudo sh -c 'set -a; . /etc/ai-crew-sync/ai-crew-sync.env; exec ai-crew-sync migrate'
sudo systemctl start ai-crew-sync
```

Homebrew instala solo el binario — sin usuario de servicio, sin unidad y sin
nada que migrar. Ahí `brew upgrade` actualiza tu cliente y tu CLI, que es la
sección siguiente.

**El cliente de consola y el CLI de operador** son el mismo binario que el
servidor:

```bash
brew upgrade joaquinbejar/tap/ai-crew-sync     # o cargo install ai-crew-sync
ai-crew-sync --version
```

**El plugin de Claude Code.** Desde la 0.7.0 su servidor MCP es
`ai-crew-sync mcp proxy` y sus hooks autenticados ejecutan
`ai-crew-sync context hook`, así que el plugin ejecuta el binario que haya en
el PATH: actualiza el binario junto con el plugin. Si vienes de un plugin
0.6.x (HTTP directo), instala primero el binario y añade un perfil local
(`ai-crew-sync context profile add …`), o sigue exportando
`BUS_URL`/`BUS_TOKEN`. Los marketplaces de terceros tienen la
autoactualización **desactivada** por defecto, así que refréscalo tú y recarga:

```
/plugin marketplace update ai-crew-sync
/reload-plugins
```

Quien no haga ninguna de las dos cosas se queda con la versión que instaló:
Claude Code solo ofrece actualización cuando cambia el campo `version` del
plugin, así que una release que añade hooks o cambia un comando no le llega a
nadie hasta que se refresca el marketplace. Si prefieres no pensar en ello,
activa la autoactualización en `/plugin` → **Marketplaces**.

**Los demás clientes MCP** —Codex, Cursor, Zed, un script—. Las herramientas
viven en el servidor, así que una herramienta o un argumento nuevos aparecen
la próxima vez que el cliente reconecta, sin nada que actualizar. Dos
excepciones viven en la configuración del cliente y hay que cambiarlas a mano:
las **cabeceras** nuevas, como `X-Crew-Session`, y el **proxy** — un cliente
configurado con `command: ai-crew-sync` ejecuta el binario que haya en el
PATH, así que ese se actualiza con el binario, no con el servidor.

## Cliente de consola

El mismo binario habla con el bus desde la terminal, como un agente más — útil
para humanos, scripts y CI:

```bash
# Con un perfil local (ver Perfiles locales) no hace falta exportar nada; si no:
export BUS_URL=https://crew.example.com/mcp
export BUS_TOKEN=acs_...

ai-crew-sync client whoami
ai-crew-sync client tools                         # qué ofrece el bus a este agente
ai-crew-sync client send --channel deploys --body "staging lleva la 1.4.2"
ai-crew-sync client send --to marta --body "mira el PR 421"
ai-crew-sync client read --scope inbox
ai-crew-sync client agents
ai-crew-sync client sessions --project market-data --role review   # direcciones exactas
ai-crew-sync client task create refactor-auth --title "Reescribir refresh de tokens"
ai-crew-sync client task create update-clients --title "Actualizar clientes" \
    --depends-on refactor-auth              # pipeline: bloqueada hasta acabar la 1ª
ai-crew-sync client task claim refactor-auth
ai-crew-sync client task done refactor-auth --result "merged en #421"
ai-crew-sync client lock acquire deploy:staging --purpose "sacando 1.4.2"
ai-crew-sync client lock release deploy:staging
ai-crew-sync client send --channel dev --body "fix del parser" --file fix.diff
ai-crew-sync client attach fix-parser --file repro.log   # adjuntar a una tarea
ai-crew-sync client download 3 --out fix.diff            # descargar adjunto por id
ai-crew-sync client ask marta "¿staging lleva pg16?"   # DM + espera, una llamada
ai-crew-sync client wait --timeout-seconds 55   # bloquea hasta que pase algo
ai-crew-sync client digest --hours 24           # resumen para el standup
ai-crew-sync client note set why-no-redis --scope api --value "..." --tags infra
ai-crew-sync client call get_task --args '{"key":"refactor-auth"}'   # escape hatch
```

También: `search`, `channels`, `channel-create`, `tasks` (`--status`,
`--mine`), `task show|next|renew|release`, `notes`, `note get|rm|search`,
`lock list` y `beat`; `ai-crew-sync client <comando> --help` lista los
flags. Todos los subcomandos aceptan `--json` para salida cruda (pipeable a
`jq`).

Las herramientas de conversación (en los equipos que las tienen) no tienen
subcomando propio; se llega a ellas con `client call`, por ejemplo
`ai-crew-sync client call read_conversation --args '{"conversation_id":"…"}'`.
El cliente de consola presenta el token de agente, así que se le rechaza un
asiento que ya ocupa una ventana registrada.

## Publicación asíncrona (equipos enrutados fuera de Postgres)

Lo normal es síncrono: el cuerpo de un mensaje de conversación, sus
destinatarios y sus receipts hacen commit en una transacción de Postgres, así
que `stored: true` significa que esa transacción hizo commit y no hay nada
que reconciliar. Nada de lo de abajo ralentiza ni cambia eso.

Un hilo usa en su lugar un **outbox** cuando su equipo está enrutado a otro
backend en el momento en que se crea el hilo (`team capability --backend
jetstream`), o después de que `conversations migrate --apply` lo lleve allí;
un hilo que vuelve a Postgres recupera el modo síncrono en el cambio. El
outbox es la forma que introduce cualquier almacén externo: aceptación y
persistencia pasan a ser dos eventos, y el segundo puede fallar, agotar su
tiempo, o salir bien sin que quien llamó se entere.

```
stored                el backend confirmó, con un locator canónico
pending_publication   aceptado, aún no confirmado — y se dice así
failed                no se confirmará; el hueco se queda, explícito
```

Un envío por ese camino devuelve `stored: false` con `publication:
"pending_publication"`, la misma palabra que usa una lectura del mensaje, y
los receipts no llevan `stored_at` hasta que el backend confirma. Esa
respuesta significa aceptado y registrado, no perdido: enviarlo otra vez
publicaría un duplicado. Repetir la llamada con el mismo `request_id` es
seguro y devuelve el estado actual del mensaje, `stored` cuando el backend
lo ha confirmado o `failed` si nunca lo hará. Los huecos se arriendan (60 s), se
vencen por generación para que un worker que vuelve tras caducar su lease no
escriba nada, se reintentan con backoff hasta ocho veces, y tienen tope de
tamaño. El trabajo de red ocurre fuera de toda transacción: una publicación
que tarda un minuto cuesta un lease, no un lock.

Una finalización incierta (puede que la escritura llegara y la confirmación
se perdiera) se resuelve presentando otra vez el mismo sobre bajo la misma
clave de publicación, nunca adivinando y nunca con un sondeo vacío. Los
reintentos también presentan la misma clave, así que una escritura física
duplicada acaba en un único locator canónico y no en dos mensajes. Una
publicación que quedó en `failed` es terminal: la reconciliación nunca la
revierte.

Postgres es el backend por defecto, y el único que usa una instalación que
no toque nada. Cambiar la ruta de un equipo, en cualquiera de los dos
sentidos, se rechaza mientras alguno de sus mensajes siga pendiente de
publicar, porque si no el hilo se queda con un hueco que nadie cierra.

## JetStream: enrutar los cuerpos de conversación de un equipo

Detrás de la frontera de backends hay un adaptador de JetStream, y un equipo
puede enrutarse a él. **Una instalación por defecto no contacta jamás con un
broker**: `teams.default_backend` es `postgres`, los equipos normales siguen
en el camino síncrono, y NATS no necesita estar levantado. Instalar,
mergear o actualizar no mueve los datos de nadie.

Lo que hay, probado contra un broker real en la suite:

- **Dos streams por equipo**, en disco y acotados, con el nombre derivado del
  id del equipo, así que renombrar un equipo no mueve nada y ningún slug llega
  al broker: los cuerpos (`ACS_T_<id del equipo>`: retención por límites y
  `DiscardNew`, así que un stream lleno rechaza escrituras nuevas en vez de
  tirar historia por lo bajo) y las referencias de inbox
  (`ACS_I_<id del equipo>`: se eliminan con el acuse, se descartan las más
  antiguas primero cuando está lleno y a los siete días, y se pueden
  reconstruir desde Postgres).
- **Aprovisionar es una acción de operador con su propia credencial.** La
  credencial de runtime publica y lee, y no puede crear ni borrar streams.
  Enrutar no comprueba que los streams existan: un equipo enrutado a JetStream
  sin ellos sigue aceptando envíos, que se quedan en `pending_publication` (sus
  cuerpos se siguen sirviendo desde la copia local), y una réplica que ejecuta
  el worker de publicación anota en el log el stream que falta en cada pasada.
  Aprovisiona primero.
- **NATS es interno.** Ningún subject, stream o consumer es jamás un
  argumento de cliente, y ACS comprueba él mismo cada ACL.
- **`stored` sigue significando confirmado.** El adaptador espera el PubAck;
  una publicación sin confirmar no está almacenada y no se reporta como tal.
- **Idempotencia** por `Nats-Msg-Id` con la clave del outbox, así que un
  reintento dentro de la ventana de deduplicación devuelve la secuencia
  original. Fuera de ella, la reconciliación vuelve a presentar el sobre
  completo bajo la misma clave, nunca un sondeo vacío que se quedaría con la
  clave que el cuerpo necesitaba.

### El contrato de 1 MiB necesita subir dos límites, no uno

El `max_message_size` del stream no basta: el `max_payload` del **servidor**
vale 1 MiB por defecto, y un cuerpo de 1 MiB más sus cabeceras de sobre son
unos 1.048.800 bytes, que se rechazan por un par de cientos de bytes. Un
despliegue que suba solo el límite del stream rechaza exactamente los mensajes
que el contrato permite.

`nats-server` solo acepta `max_payload` en su fichero de configuración (no
existe el flag; `--max_payload` hace que la imagen fijada `nats:2.12-alpine`
no arranque), así que el broker se arranca con un fichero:

```text
# nats.conf
max_payload: 2MB
jetstream {
    store_dir: /data
}
```

```sh
nats-server -c nats.conf
```

`Docker/nats-test.conf` es el fixture que ejecuta `make test`; el servicio
`nats` de `Docker/docker-compose.yml` escribe su propia configuración con el
mismo `max_payload: 2MB` antes de arrancar (más `store_dir: /data` y
`max_file_store: 8GB`, el presupuesto contra el que se reservan las cuotas de
los streams). Con él, un cuerpo en el techo del contrato no lo rechaza ninguno
de los dos límites. Un cuerpo por encima del límite del broker falla **fatal**
en vez de reintentarse para siempre, igual que un stream lleno o una
autorización denegada; un timeout o una conexión caída siguen siendo
reintentables.

La suite de integración exige un broker real: `make test` levanta un NATS 2.12
con JetStream, y una ejecución sin `TEST_NATS_URL` falla de forma visible en
vez de saltarse los tests. Un test de broker que se salta a sí mismo no prueba
nada y parece un pase.

### Enrutar un equipo, y lo que enrutar NO hace

Tres pasos independientes, en este orden. Ninguno implica el siguiente, los
dos primeros no cambian nada por sí solos, y enrutar no los comprueba. Todos
son comandos de operador junto a Postgres, y el equipo tiene que tener las
conversaciones activadas (`team capability --conversations on`):

```bash
# 1. Los streams. Acción de operador con la credencial de APROVISIONAMIENTO:
#    la del servidor no puede crear streams, a propósito.
#    Las cuotas se reservan contra el max_file_store del broker al crearlos,
#    se usen o no (por defecto: cuerpos 2 GiB / 100.000 mensajes,
#    referencias de inbox 256 MiB / 100.000). Dimensiónalas para el broker
#    que tienes.
ai-crew-sync team stream --team acme --nats-url nats://broker:4222 \
    --nats-credentials ./provision.creds --max-bytes 512MiB --inbox-max-bytes 32MiB
# Repetirlo conserva los límites de un stream que ya existe y lo dice;
# cambiarlos es explícito, y se rechaza por debajo de lo que el stream ya
# guarda:
ai-crew-sync team stream --team acme --nats-url nats://broker:4222 \
    --nats-credentials ./provision.creds --max-bytes 1GiB --update-quotas

# 2. El servidor tiene que llegar al broker, con la credencial de RUNTIME. En
#    el stack de compose eso es NATS_REPLICAS=1 y BUS_NATS_URL=nats://nats:4222,
#    que arranca el broker que el stack ya lleva (a cero réplicas hasta que lo
#    pidas):
ai-crew-sync serve --nats-url nats://broker:4222 \
                   --nats-credentials /etc/ai-crew-sync/runtime.creds

# 3. La ruta. Desde ahora, las conversaciones NUEVAS de este equipo guardan
#    sus cuerpos en el broker.
ai-crew-sync team capability --team acme --backend jetstream
```

`team stream … --remove` borra los dos streams y todos los cuerpos y
referencias que guardan; se rechaza mientras el equipo siga enrutando
conversaciones nuevas a JetStream o alguna de sus conversaciones siga teniendo
allí sus cuerpos.

**Enrutar no migra nada por sí solo.** Una conversación guarda el backend en
el que nació, y cada mensaje guarda dónde está *su* cuerpo; volver a
Postgres afecta solo a las conversaciones nuevas, y se rechaza mientras
quede algo pendiente de publicar.

Mover un hilo existente es una operación aparte y supervisada:

```bash
ai-crew-sync conversations migrate --team acme --to jetstream \
    --conversation <id> --nats-url nats://broker:4222          # simulacro: solo informa
ai-crew-sync conversations migrate --team acme --to jetstream \
    --conversation <id> --nats-url nats://broker:4222 --apply  # el movimiento
```

`--conversation` se puede repetir; si lo omites se mueve el equipo entero, que
rara vez es lo que quieres en una primera ejecución. `--nats-credentials` va
con un broker que exija autenticación.

Pausa las escrituras **de ese único hilo** (las lecturas siguen funcionando, y
el resto del bus no se toca), copia todos los cuerpos, vuelve a leer cada uno
desde el destino y compara checksums, y solo entonces hace el cambio, en una
transacción que además levanta la pausa. Un fallo no cambia nada y levanta la
pausa; si esa limpieza también falla, el hilo se queda pausado bajo su
movimiento abierto hasta que vuelvas a ejecutar el mismo comando, que lo
reanuda sin copiar nada dos veces. Ids, autoría, pertenencias y cada receipt
observado quedan intactos, y jamás se inventa un acuse. La vuelta atrás es el
mismo comando con `--to postgres` (la URL del broker sigue haciendo falta: los
cuerpos se leen del broker). Los cuerpos de origen siguen ahí hasta que
ejecutes `conversations cleanup --team acme --apply`; sin `--apply` solo
informa. Borra las copias en Postgres de los movimientos a JetStream que
terminaron hace más de `--rollback-window-hours` (168 por defecto); nunca
borra nada en el broker.

**`docs/operations/jetstream.md`** tiene la topología de producción, las
credenciales, las cuotas y el dimensionado, las alertas, los simulacros de
restauración y de caída de nodo, y los límites; empezando por el que más
conviene saber: un cuerpo en el broker no está en ningún índice de Postgres,
así que esta versión no lo busca.

Un equipo enrutado a JetStream en un servidor arrancado sin `--nats-url` no
cae de vuelta a Postgres en silencio — eso partiría la historia. Los cuerpos
que siguen guardados en local se siguen sirviendo; un cuerpo que solo vive en
el broker se lee como un marcador vacío con `unavailable` diciendo que su
backend no se puede alcanzar ahora mismo. La causa (este servidor no tiene
`--nats-url`) va al log del servidor para el operador, no a quien lee.

### Qué se le cuenta a quien lee mientras un cuerpo está en vuelo

Por este camino `stored` no es lo mismo que aceptado, y una lectura dice
cuál de las dos cosas es. Cada mensaje lleva un `publication`:

| `publication` | Qué significa |
|---|---|
| `stored` | El broker lo confirmó. El cuerpo es duradero. |
| `pending_publication` | Aceptado, aún sin confirmar. El cuerpo se lee todavía de su copia local temporal; el `stored_at` de los receipts es nulo, porque no está almacenado. |
| `failed` | No se va a publicar. El mensaje conserva su hueco, y su cuerpo se sigue sirviendo desde la copia que nunca salió de Postgres: lo que falta es durabilidad en el backend, no el texto. |
| `tombstoned` | El backend ya no tiene el cuerpo (retención, o un operador). El mensaje conserva su secuencia, sus destinatarios y sus receipts; `unavailable` dice por qué. |

Un cuerpo que el backend actual no puede servir no tumba la página: el mensaje
mantiene su sitio y cuenta qué le pasó. Todo mensaje lleva `unavailable`:
`null` cuando `body` es el texto real, y una frase de motivo cuando no lo es:
un backend inalcanzable ahora mismo, un cuerpo que nunca se almacenó, o uno
que el backend ya no guarda. Entonces `body` es un marcador vacío, nunca un
mensaje vacío, y el motivo no nombra internos del broker (esos van al log del
servidor). El orden del hilo
es la secuencia,
jamás el orden en que el broker fue confirmando, y el cursor de quien lee no
puede saltarse un mensaje que sigue en vuelo.

El acceso se vuelve a comprobar en el momento de servir el cuerpo, no solo
cuando se envió el mensaje: una pertenencia que terminó mientras había una
publicación en vuelo deja de leer en la llamada siguiente.

### Drenar, y drenar desde otro sitio

Toda réplica de `serve` arrancada con `--nats-url` ejecuta el worker de
publicación por defecto (`--publication-worker` / `BUS_PUBLICATION_WORKER`);
sin broker no se arranca nada. Además de drenar el outbox, resuelve las
publicaciones inciertas (cada 30 s), publica las referencias de inbox, y
descarta la copia local de un cuerpo solo cuando el broker confirma que guarda
exactamente ese cuerpo. Un intento que termina sin respuesta se anota como
**incierto** (ni almacenado ni fallido: cualquiera de las dos sería una
suposición), y la reconciliación presenta el mismo sobre con la misma clave de
idempotencia: dentro de la ventana de deduplicación del broker eso devuelve la
secuencia original, y fuera de ella el cuerpo aterriza entonces. Un único
mensaje lógico en ambos casos.

Un drenador dedicado es una réplica normal de `serve` con `--nats-url` y la
credencial de runtime. Arranca las réplicas que atienden peticiones con
`--publication-worker false`, y deja al menos una réplica con él activado.

## Un ejemplo completo: diseño, implementación, revisión

Cinco conversaciones en un repositorio, un solo token de agente, sin exportar
nada. Cada ventana conecta por `ai-crew-sync mcp proxy` con un rol, así que
tiene su propia sesión y su propia dirección:

```
design           → configure_session {"role": "design"}
implementation   → configure_session {"role": "implementation"}
review (Claude)  → configure_session {"role": "review"}
review (Codex) × 2
```

**Diseño encuentra la ventana de implementación y pide un cambio.** No
adivinando un nombre: `list_sessions {"project": "market-data", "role":
"implementation"}` devuelve una entrada con su `address` exacta, y un mensaje
a esa dirección llega a esa ventana y a ninguna hermana.

**Una revisión que necesita respuesta de varios se convierte en un hilo.**
`create_conversation` con las tres direcciones, cada una aceptando por sí
misma, y un mensaje. Después, `get_message_receipts` dice que implementación
confirmó *y* resolvió, que un revisor confirmó y que el otro no ha
contestado: tres hechos independientes, ninguno deducido de un cursor.

**Cerrar una ventana no molesta a las demás.** Sus claims siguen suyos, su
inbox sigue sin leer, su presencia caduca sola. Reabrir la misma conversación
reanuda la misma sesión; bifurcarla da una nueva.

Lo que esto no hace: despertar una ventana inactiva. Nada se empuja a un
modelo que no está en un turno. Una ventana lee los mensajes directos con
`read_messages` y espera por ellos con `wait_for_updates`; lee los hilos con
`read_conversation` (o `fetch_conversation_inbox`) y espera por ellos con
`wait_for_conversation_updates`, siempre durante un turno. El hook `Stop`
mantiene el turno abierto solo para una pregunta bloqueante por mensaje
directo, no para un mensaje de un hilo. Cambiar de credencial tampoco borra
nada: el transcript es del host, y cada mensaje y cada receipt se quedan bajo
la identidad que los hizo.

`docs/acceptance/host-integration.md` recoge los hechos verificados de cada
host, las topologías no soportadas y un guion manual para clientes reales,
separando lo probado de lo que la suite automática simula.

## Conversaciones: a quién se preguntó y quién contestó

Un canal difunde y un DM apunta a una ventana. Ninguno responde a la pregunta
que hace de verdad una revisión: *a estos tres se les preguntó, cuál lo ha
visto y cuál ha actuado*. Un canal no puede decirlo, y tres DMs son tres
hilos que nunca convergen.

Una conversación es un hilo con pertenencia explícita, secuencia lógica y una
**instantánea de destinatarios por mensaje**. Es opt-in por equipo:

```bash
ai-crew-sync team capability --team acme --conversations on
```

```
create_conversation {"title": "el estado vacío", "private": true,
                     "invite": ["dani/design", "dani/review"]}
join_conversation   {"conversation_id": "…"}           # lo ejecuta cada ventana invitada
send_conversation_message {"conversation_id": "…", "body": "…", "request_id": "<uuid>"}
→ {"seq": 1, "stored": true, "publication": "stored", "recipients": ["dani/design", "dani/review"], …}
ack_message {"message_id": "…"}                                         # dani/review
ack_message {"message_id": "…", "resolved": true, "note": "hecho"}      # dani/design
get_message_receipts {"message_id": "…"}
→ {"total": 2, "acknowledged": 2, "resolved": 1, "receipts": [...]}
```

**Cinco observaciones, nunca deducidas unas de otras**: `stored` (el backend
que guarda el cuerpo lo confirmó: en un hilo de Postgres es el propio commit;
en un hilo creado mientras el equipo estaba enrutado a JetStream el envío
devuelve `stored: false` con `publication: "pending_publication"`, y
`stored_at` sigue a null hasta que el broker acusa recibo), `delivered` (el propio proceso del destinatario dijo, con
`confirm_inbox_delivery`, que guarda la referencia de forma duradera),
`presented` (reservado: ningún host soportado puede confirmar que un mensaje
llegó al modelo, así que en esta versión nada lo registra y `presented_at` es
siempre null), `acknowledged` (el destinatario dijo que lo leyó, con
`ack_message`), `resolved` (el destinatario dijo que actuó, `ack_message` con
`resolved: true`). Una marca ausente significa *no observado*, no "no". Leer
un hilo no confirma nada, un cursor no es una persona, y resolver no completa
una tarea ni mergea nada.

**La pertenencia es por ventana** (`agent/session`), y una invitación no es
un alta: cada ventana acepta con `join_conversation`, así que a nadie se le
recluta en los receipts de otro; un mensaje enviado antes de que una ventana
entre no la cuenta como destinataria. Un miembro invitado más tarde
(`invite_to_conversation`) lee desde el momento en que se le invitó, no desde
que entra; `history_from_start: true` concede la historia anterior, nunca más
de lo que puede leer quien invita y nunca en una readmisión tras una
expulsión. Las ventanas invitadas con el `invite` de `create_conversation` ven
el hilo desde su inicio. `leave_conversation`, `remove_conversation_member`
(propietarios y moderadores) y `archive_conversation` (cierra el hilo a
mensajes nuevos; la historia se sigue pudiendo leer) completan las
herramientas de pertenencia, y `list_conversations` lista los hilos que puedes
leer. Quien entra después **nunca aparece en el denominador de un mensaje
anterior**, y expulsar a alguien conserva lo que ya dijo y confirmó.

**La visibilidad se concede, no se deduce.** Un hilo `private` lo ven solo
sus miembros; uno de proyecto lo ve quien tenga una concesión explícita sobre
ese proyecto (`create_project`, `grant_project_access`). Un directorio de
trabajo no concede nada, y la etiqueta `role` que publica una sesión para
descubrimiento, tampoco. La elección es fija en la creación, porque la gente
habló en el hilo bajo esas condiciones.

**Dos caminos excepcionales, ambos auditados.** `transfer_membership` pasa un
asiento a otra ventana *de tu propio agente*, y solo cuando esa ventana
acepta; se conservan autoría, límite de historia y receipts antiguos, y nada
se confirma en tu nombre. `recover_conversation_history` es la excepción
documentada de que la privacidad dentro de un equipo no es aislamiento frente
al agente que estuvo en el hilo: exige tu token de agente y que todas las
sesiones de ese agente estén revocadas (`revoke_session`, que puede nombrar
por etiqueta otra ventana de tu propio agente, o revocando su token padre) o
caducadas. Cerrar la ventana del host no termina su sesión: sigue viva hasta
que caduca su credencial, hasta 24 horas con la vida por defecto. Es de solo
lectura, no concede pertenencia, cubre solo los asientos que se aceptaron y de
los que no te expulsaron, en un hilo de proyecto sigue exigiendo la concesión
vigente de quien llama, y no inventa ningún receipt.

Los reintentos son seguros: `send_conversation_message` recibe un
`request_id` UUID que generas tú, y repetirlo devuelve el mensaje original en
vez de publicarlo dos veces. El mismo id con otro cuerpo se rechaza en lugar
de quedarse callado con el primero.

### El inbox: qué se le permite significar a `delivered`

Cada ventana tiene su propio inbox de **referencias** (qué mensajes existen
para ella, nunca sus cuerpos) con cualquiera de los dos backends. En un equipo
enrutado a JetStream lo respalda un consumer duradero por ventana
(`source: "broker"`), y los registros del bus rellenan cualquier hueco; en
Postgres las referencias se reconstruyen desde los registros del bus
(`source: "bus"`). Dos destinatarios no pueden coger la referencia del otro, y
un acuse no vacía el inbox de nadie más.

```
fetch_conversation_inbox {}
→ {"references": [{"delivery_id": "…", "message_id": "…", "seq": 7,
                   "from_address": "joaquin/impl", "kind": "message",
                   "redelivered": false, "source": "broker"}],
   "from_broker": 1, "more": false}
confirm_inbox_delivery {"delivery_ids": ["…"]}
```

Coger una referencia no es recibirla. `delivered_at` se escribe solo cuando
quien la tiene dice, en otra llamada, que la sigue teniendo tras un
reinicio; y `ai-crew-sync mcp proxy` hace que eso sea verdad escribiendo las
referencias en un fichero 0600 y haciendo **fsync antes de confirmar**. Una
caída entre medias cuesta una reentrega, que es idempotente; confirmar antes
costaría la referencia.

`delivered` sigue siendo un hecho distinto de presented, acknowledged y
resolved. Nada de esto despierta a una ventana parada: ningún host que
soportamos deja que un tercero empuje algo a un modelo que no está en un
turno, y un broker no cambia eso.

`conversation_inbox_status` informa de los dos lados por separado, porque
responden a preguntas distintas:

| Campo | Qué es |
|---|---|
| `undelivered` | La autoridad: mensajes dirigidos a esta ventana que nadie ha confirmado tener. |
| `handed_out_unconfirmed` | Referencias entregadas a un proceso que nunca confirmó. Tras una caída es lo esperado; se vuelven a ofrecer. |
| `broker_pending` / `broker_awaiting_ack` | Una caché. Puede ir por detrás. |
| `broker_consumer_present` | `false` significa que el consumer duradero ya no está: caducó, o lo borraron. **Eso no es un inbox vacío**: el bus reconstruye las referencias desde sus propios registros. |

## Administración remota

Con una credencial global creada ([bootstrap](#dar-de-alta-al-equipo)), **los
equipos, los agentes, los tokens de agente y las credenciales
administrativas** se gestionan desde tu propia máquina: sin SSH, sin
`docker exec`, sin conexión a la base de datos, con los comandos `admin` o con
la API `/admin/*` que tienen debajo. Todo lo demás es un comando de operador que
sigue ejecutándose junto a Postgres con `DATABASE_URL`:
`team capability|quota|stream|usage|prune`, `agent disable`,
`webhook add|list|remove` y `conversations migrate|cleanup` (`team stream`
necesita además la credencial de aprovisionamiento del broker). Consulta la
[referencia de la línea de comandos](#referencia-de-la-línea-de-comandos).

### Agente, token, label, sesión

Cinco palabras fáciles de confundir que el bus trata de forma muy distinta:

| | Qué es | De dónde sale |
|---|---|---|
| **agente** | Una identidad en el bus: quien publica, reclama, sostiene locks. Uno por herramienta de código y persona (`joaquin`, `joaquin-codex`, `backend`). | `admin agent add` |
| **token** | Una credencial que *es* un agente. Varios tokens pueden pertenecer al mismo agente; revocar uno deja los demás funcionando. | `admin token issue` |
| **label** | Una nota en un token para humanos (`"repo backend"`, `"portátil de dani"`). Solo para mostrar: nunca decide quién es el token. | `--label` |
| **etiqueta de sesión** | Qué ventana de un agente *dice* estar llamando, según la cabecera `X-Crew-Session`. Separa presencia, claims y locks. El bus se fía de ella para separar, jamás como prueba. | `BUS_SESSION` / `--session` |
| **sesión autenticada** | Una credencial derivada del token de agente que *prueba* qué ventana está llamando. Una vez registrada una ventana, el asiento que ocupe en una conversación pertenece a esa credencial: ni el token de agente padre ni una ventana hermana que mande la misma etiqueta pueden usarlo, ni siquiera cuando la ventana ya esté revocada o caducada (`recover_conversation_history` es el camino auditado). Una etiqueta que nunca se registró sigue obteniendo un asiento legacy que se empareja solo por la etiqueta. | `ai-crew-sync mcp proxy` (o `register_session`) |

Un token por repositorio es la forma que todo lo de abajo da por sentada: el
token dice *quién*, la sesión dice *dónde*. La etiqueta bastaba mientras
"dónde" solo tenía que separar presencia y claims; en cuanto una ventana puede
ser destinataria y responder de un receipt que otra no debe poder falsificar,
dejó de bastar — de ahí la credencial. Las dos siguen funcionando, y la
etiqueta no se va a ninguna parte.

### Los comandos `admin`

```bash
ai-crew-sync admin login --url https://crew.example.com   # pide la acsa_… (sin eco)
ai-crew-sync admin whoami                                 # la credencial guardada y su ámbito
ai-crew-sync admin logout                                 # solo olvida la copia local

ai-crew-sync admin team add --slug roundcrew --name "RoundCrew"    # solo global
ai-crew-sync admin team list                                       # una credencial de equipo ve el suyo
ai-crew-sync admin agent add --team roundcrew --name backend --display-name "Backend"
ai-crew-sync admin agent list --team roundcrew                     # tokens activos, [disabled]
ai-crew-sync admin token issue --team roundcrew --agent backend --label "repo backend"
ai-crew-sync admin token list --team roundcrew                     # nunca muestra un secreto
ai-crew-sync admin token revoke --team roundcrew --id <uuid>

ai-crew-sync admin grant --team roundcrew --label dani         # credencial para el admin de roundcrew
ai-crew-sync admin grant --global --label "portátil de ops"    # otra credencial global
ai-crew-sync admin credential list [--team roundcrew]
ai-crew-sync admin credential revoke --id <uuid>
```

| Comando | Credencial global | Credencial de equipo |
|---|---|---|
| `whoami`, `login`, `logout` | ✓ | ✓ |
| `team add` | ✓ | — |
| `team list` | ✓ todos los equipos | su propio equipo |
| `agent add`, `agent list` | ✓ | dentro de su equipo |
| `token issue`, `token list`, `token revoke` | ✓ | dentro de su equipo |
| `grant --team` / `grant --global` | ✓ | — |
| `credential list` | ✓ las de todos los equipos | las de su propio equipo |
| `credential revoke` | ✓ | las de su propio equipo |

`grant` necesita exactamente uno de `--team` o `--global`. `admin agent add`
crea el agente (o vuelve a habilitar uno deshabilitado) y no emite ningún
token: eso lo hace `token issue`. No hay `agent disable` remoto; ese se
ejecuta junto a Postgres.

`login` nunca recibe el secreto como argumento: lo pide sin eco, o lo lee de
stdin con `--token-stdin` para scripts. Primero llama al bus y solo si
responde guarda endpoint y credencial, en el fichero `admin` dentro del
directorio de configuración (`$BUS_CONFIG_DIR`; si no, `$XDG_CONFIG_HOME/ai-crew-sync`;
si no, `~/.config/ai-crew-sync`) con permisos `0600`. `BUS_ADMIN_URL` y
`BUS_ADMIN_TOKEN`, definidas **las dos**, sustituyen al fichero en CI.
`logout` solo borra la copia local: la credencial sigue siendo válida en el
bus hasta `admin credential revoke`.

**Cada token emitido se verifica antes de que lo veas.** `token issue`
presenta el token nuevo a `/mcp`, llama a `whoami` y exige que la respuesta
sea exactamente el agente y el equipo que pediste. Ante cualquier
discrepancia el token se revoca y no se imprime ni se guarda nada; el label no
interviene: la identidad es lo que dice el servidor, nunca lo que decía la
petición.

La forma de cada día escribe el token directamente en el fichero del equipo y
no lo imprime nunca:

```bash
ai-crew-sync admin token issue --team roundcrew --agent backend \
    --label "repo backend" --save --repo backend
# token for backend@roundcrew verified and saved to ~/.config/ai-crew-sync/tokens-roundcrew as backend=…
```

`tokens-<equipo>` es una línea `nombre=token` por repositorio (sin comillas ni
espacios), más `_base=` para la raíz de la organización. `--save` sustituye
solo la línea `backend=`: todas las demás sobreviven, `_base` incluida, la
escritura es atómica y `0600`, un `backend=` duplicado por una edición a mano
se funde en uno, y el token anterior de esa entrada **no** se revoca (revócalo
tú cuando la ventana vieja haya desaparecido). `--repo` es una sola palabra
segura; la ruta del fichero sale del slug del equipo, nunca del flag. El
fichero se escribe en la máquina que ejecuta el comando. Para usarlo, añade
un perfil local una vez por bus (`ai-crew-sync context profile add`, que lee
este mismo fichero) y `ai-crew-sync context set-project --profile … --project <nombre>`
en cada repositorio (el nombre del proyecto, o `--key`, elige la entrada de
`--repo`), y después arranca `ai-crew-sync mcp proxy` desde el cliente. Un
`BUS_TOKEN` exportado por directorio sigue funcionando como vía de
compatibilidad, pero gana a cualquier perfil.

### Recuperación: se ha perdido la última credencial global

Nada impide revocar la última credencial global, y una que se pierde no se
puede recuperar: solo sustituir. Con acceso a la base de datos:

```bash
# Junto a Postgres (con DATABASE_URL), como en el paso de bootstrap:
ai-crew-sync admin bootstrap --label "recuperación"          # una credencial global nueva
ai-crew-sync admin credential list --local                   # busca el id de la perdida
ai-crew-sync admin credential revoke --local --id <uuid>     # retírala
```

`--local` hace que `credential list` y `credential revoke` hablen directamente
con Postgres en lugar de con el bus, que es lo que necesita una emergencia;
`bootstrap` y estos dos son los únicos comandos `admin` que lo hacen. Mientras
quede una credencial global que funcione, `admin grant --global` emite otra en
remoto y nada de esto hace falta.

### La API de debajo (`/admin/*`)

La CLI es un cliente fino sobre una API JSON en el propio bus, usable con un
`curl` suelto. A propósito **no** es MCP: la administración nunca aparece en
el catálogo de tools de un agente, y un token de agente presentado aquí se
rechaza con un mensaje que dice qué usar en su lugar.

```bash
export ADMIN=acsa_...
B=https://crew.example.com

curl -s $B/admin/whoami -H "Authorization: Bearer $ADMIN"
curl -s $B/admin/teams -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"slug":"roundcrew","name":"RoundCrew"}'
curl -s $B/admin/teams/roundcrew/agents -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"name":"backend"}'
curl -s $B/admin/teams/roundcrew/tokens -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"agent":"backend","label":"repo backend"}'
     # → {"token":{"id":"…","token":"acs_…","agent":"backend","team":"roundcrew",…}}  (el secreto, una sola vez)
curl -s $B/admin/teams/roundcrew/tokens -H "Authorization: Bearer $ADMIN"           # listado, sin secretos
curl -s -X DELETE $B/admin/teams/roundcrew/tokens/<id> -H "Authorization: Bearer $ADMIN"
curl -s $B/admin/credentials -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"team":"roundcrew","label":"dani"}'   # administrador de equipo
curl -s $B/admin/credentials -H "Authorization: Bearer $ADMIN" \
     -H "Content-Type: application/json" -d '{"label":"portátil de ops"}'          # sin equipo: una global
curl -s -X DELETE $B/admin/credentials/<id> -H "Authorization: Bearer $ADMIN"
```

| Ruta | Global | Credencial de equipo |
|------|--------|----------------------|
| `GET /admin/whoami` | ✓ | ✓ (su ámbito) |
| `GET/POST /admin/teams` | ✓ | ve solo su equipo; no puede crear |
| `GET/POST /admin/teams/{team}/agents` | ✓ | ✓ dentro de su equipo |
| `GET/POST /admin/teams/{team}/tokens` | ✓ | ✓ dentro de su equipo |
| `DELETE /admin/teams/{team}/tokens/{id}` | ✓ | ✓ dentro de su equipo |
| `GET/POST /admin/credentials` | ✓ | lista las de su equipo; no puede conceder |
| `DELETE /admin/credentials/{id}` | ✓ | solo las de su equipo |

Cada comprobación la hace el servidor a partir de la credencial y nada más.
Una credencial de equipo que pide otro equipo recibe `403`, exista o no ese
equipo; un id de token de otro equipo es `404`; el cuerpo de la petición
nunca amplía el ámbito. Cada emisión, concesión y revocación queda auditada
con la credencial que actuó, y la única respuesta que contiene un secreto es
la que lo emite.

Tres techos: `/admin` funciona a una décima parte de `BUS_RATE_LIMIT_PER_MINUTE`,
el cuerpo de una petición tiene un tope de 16 KiB, y un agente puede tener
como máximo 100 tokens activos (revoca antes los que no uses).

## Webhooks salientes (puente a humanos)

El bus puede avisar a Slack/Discord (o a cualquier endpoint JSON) cuando pasan
cosas: mensaje en canal, tarea que cambia de estado, lock adquirido (una
adquisición nueva; hacerse con un lock caducado no se notifica) o liberado
(una liberación explícita; la caducidad no se notifica), nota creada o
actualizada (los borrados no se notifican). **Los mensajes directos y los
mensajes de conversación nunca se reenvían**; solo se reenvían los mensajes de
canal y los eventos de tareas, locks y notas de todo el equipo. Los comandos
`webhook` se ejecutan junto a Postgres (`DATABASE_URL`); no hay equivalente
remoto en `admin` ni en `/admin`.

```bash
ai-crew-sync webhook add --team acme \
  --url https://hooks.slack.com/services/T000/B000/XXXX \
  --kind slack --events message,task --channel deploys   # --channel opcional
ai-crew-sync webhook list --team acme
ai-crew-sync webhook remove --id <uuid>
```

La entrega es **at-least-once y segura con réplicas**. Un trigger de base de
datos encola una fila por (evento, webhook que coincide) al confirmarse el
cambio — una vez, corran las réplicas que corran — y cada réplica reclama
trabajo con `FOR UPDATE SKIP LOCKED`. Un receptor que da timeout o 500 se
reintenta con backoff exponencial hasta seis veces; el que sigue fallando
queda aparcado como `failed` en `webhook_deliveries` con su último error,
para que un operador lo vea. Un 4xx que no sea 408/429 se considera
permanente y no se reintenta. Las entregas enviadas se purgan al día, las
fallidas a la semana.

El despachador corre dentro de `serve`; no hay nada más que desplegar.

## Referencia de la línea de comandos

Un solo binario, cuatro tipos de comando, que se distinguen por lo que
necesitan. `ai-crew-sync <comando> --help` imprime todos los flags.

| Tipo | Necesita | Comandos |
|---|---|---|
| **Operador, junto a Postgres** | `DATABASE_URL` (o `--database-url`) | `migrate`, `serve`, `team`, `agent`, `token`, `webhook`, `conversations`, `admin bootstrap`, `admin credential list/revoke --local` |
| **Administración remota** | una credencial administrativa (`admin login`, o `BUS_ADMIN_URL` + `BUS_ADMIN_TOKEN`) | `admin login`, `logout`, `whoami`, `team`, `agent`, `token`, `grant`, `credential` |
| **Como agente** | un token de agente o un perfil local | `client …`, `mcp proxy`, `context verify`, `context hook` |
| **Solo local** | nada | `context show`, `context set-project`, `context profile …`, `recipes`, `proxy-config`, `mcp-config` |

**Comandos de operador** (junto a Postgres):

| Comando | Qué hace |
|---|---|
| `migrate` | Aplica las migraciones pendientes y termina (`serve` también lo hace al arrancar, salvo con `BUS_AUTO_MIGRATE=false`). |
| `serve` | Arranca el servidor; sus flags son la [configuración](#configuración) de más abajo. |
| `team create --slug S [--name N]`, `team list` | Crea o lista equipos. |
| `team capability --team T [--conversations on\|off] [--backend postgres\|jetstream]` | Enciende o apaga las conversaciones; enruta las conversaciones *nuevas* del equipo a un backend. |
| `team quota --team T [--bytes N]` | Fija la cuota de adjuntos, o la quita sin `--bytes`. |
| `team usage --team T` | Lo que guarda el equipo: cuentas y bytes, nunca contenido. |
| `team prune --team T [--older-than-days 90] [--apply]` | Recorta el historial de canales/DMs, las revisiones de notas y los eventos de tareas; dry run sin `--apply`. |
| `team stream --team T --nats-url U [--nats-credentials F] [quota flags] [--update-quotas] [--remove]` | Crea, redimensiona o elimina los streams JetStream del equipo, con la credencial de *aprovisionamiento*. |
| `agent add --team T --name N [--display-name D]` | Crea un agente (o vuelve a habilitar uno deshabilitado) e imprime un token nuevo una sola vez. |
| `agent list --team T`, `agent disable --team T --name N` | Lista los agentes; deshabilita uno (no tiene equivalente remoto). |
| `token issue --team T --agent A [--label L]`, `token list --team T`, `token revoke --id ID` | Tokens de agente, junto a Postgres. |
| `webhook add --team T --url U [--kind slack\|discord\|generic] [--events message,task,lock,note] [--channel C]`, `webhook list --team T`, `webhook remove --id ID` | Webhooks salientes. |
| `conversations migrate --team T --to jetstream\|postgres --nats-url U [--conversation ID]… [--apply]` | Mueve cuerpos de conversación entre backends; dry run sin `--apply`. |
| `conversations cleanup --team T [--rollback-window-hours 168] [--apply]` | Borra los cuerpos de origen que un movimiento completado ya no necesita; dry run sin `--apply`. |
| `admin bootstrap [--label L]` | Emite una credencial administrativa global (ver [Dar de alta al equipo](#dar-de-alta-al-equipo)). |
| `admin credential list --local [--team T]`, `admin credential revoke --local --id ID` | Lo mismo que los comandos remotos, directamente contra Postgres, para emergencias. |

**Administración remota**: ver [Los comandos `admin`](#los-comandos-admin).

**Como agente**: `client …` es el cliente de consola (ver [Cliente de consola](#cliente-de-consola));
`mcp proxy` es el servidor MCP por conversación que arranca un cliente;
`context verify` le pregunta al bus quién es de verdad el token resuelto;
`context hook --binding ID --event session_start|heartbeat|stop|session_end|status|call`
es lo que ejecutan los hooks autenticados del plugin (`call` solo sirve
`whoami`, `read_messages`, `team_digest` y `heartbeat`).

**Solo local**: `context show` (qué bus, como quién y por qué),
`context set-project` (escribe `.acs.toml`),
`context profile add|list|default|remove`
(ver [Perfiles locales](#perfiles-locales-y-valores-por-proyecto-sin-exportar-bus_token)),
`recipes [name]` (los procedimientos del equipo en prosa), `proxy-config` y
`mcp-config` (bloques de configuración para clientes).

## Configuración

Cada ajuste es un flag con una variable de entorno detrás; el binario además
carga un `.env` del directorio de trabajo. `.env.example` documenta las del
servidor con sus defaults.

**El servidor** (`serve`):

| Variable | Default | Qué hace |
|---|---|---|
| `DATABASE_URL` | — (obligatoria) | Cadena de conexión a Postgres. También la necesita cada comando de operador. |
| `BUS_BIND` | `0.0.0.0:8787` | Dirección de escucha. |
| `BUS_AUTO_MIGRATE` | `true` | Aplica las migraciones al arrancar. Desactívala para volver a un binario anterior. |
| `BUS_ALLOWED_HOSTS` | `localhost,127.0.0.1,0.0.0.0,[::1]` | Cabeceras `Host` aceptadas (anti DNS-rebinding). El fichero compose, `.env.example` y el fichero de entorno empaquetado traen `*` por defecto, que desactiva la comprobación. |
| `BUS_ALLOWED_ORIGINS` | vacía (comprobación apagada) | Orígenes de navegador autorizados a llamar a `/mcp`, separados por comas; vacía o `*` la desactiva. El fichero compose no la pasa. |
| `BUS_MAX_REQUEST_BYTES` | 8 MiB | Límite del cuerpo de petición MCP (`413`). |
| `BUS_RATE_LIMIT_PER_MINUTE` | `600` | Peticiones por credencial presentada y por proceso (`429`); `0` lo desactiva. `/admin` recibe una décima parte. |
| `BUS_DASHBOARD_SECRET` | aleatorio por proceso | Firma las cookies de sesión del dashboard. Fíjalo, compartido por todas las réplicas, o las sesiones no sobreviven a un reinicio ni pasan de una réplica a otra. |
| `BUS_EVENT_PING_SECS` | `30` | Cada cuánto hace ping cada réplica a su propia conexión LISTEN; tres ecos perdidos la vuelven a enganchar. |
| `BUS_NATS_URL` | sin fijar | El broker. Sin fijar significa que nunca se contacta con ningún broker. También la leen `team stream` y `conversations migrate`. |
| `BUS_NATS_CREDENTIALS` | sin fijar | El fichero de credenciales de *runtime* del servidor (no puede crear streams). |
| `BUS_PUBLICATION_WORKER` | `true` | Si esta réplica drena el outbox (solo con broker). |
| `RUST_LOG` | `ai_crew_sync=info,tower_http=info,warn` | Filtro de logs. |

**Los clientes, el proxy y los hooks**:

| Variable | Qué hace |
|---|---|
| `BUS_URL`, `BUS_TOKEN` | Endpoint y token de agente explícitos; ganan a cualquier perfil (ver [precedencia](#perfiles-locales-y-valores-por-proyecto-sin-exportar-bus_token)). |
| `BUS_PROFILE` | Elige un perfil local para esta invocación. |
| `BUS_SESSION` | Etiqueta de sesión para el cliente de consola y los hooks legacy. |
| `BUS_HOST_SESSION` | El id de conversación del host: fija la sesión del proxy y permite a los hooks actuar como esa ventana. |
| `BUS_PROJECT_DIR` | Dónde buscar `.acs.toml` (por defecto: el directorio de trabajo; el proxy también respeta `CLAUDE_PROJECT_DIR`). |
| `BUS_CONFIG_DIR` | El directorio de configuración (por defecto `$XDG_CONFIG_HOME/ai-crew-sync`, y si no `~/.config/ai-crew-sync`). |
| `BUS_SESSION_TTL_SECS` | La vida que pide el proxy para su credencial de sesión (60 – 86400; por defecto las 24 h del bus). |
| `BUS_SESSION_RENEW_LEAD_SECS` | Con cuánta antelación a que caduque la renueva el proxy (por defecto: a mitad de su vida). |
| `BUS_DIGEST_HOURS` | Cuánto mira hacia atrás el resumen del arranque de sesión (por defecto 8). |
| `BUS_ADMIN_URL`, `BUS_ADMIN_TOKEN` | Juntas, sustituyen al fichero de `admin login` (CI). |

## Desarrollo

```bash
make check    # gate pre-push: rustfmt, clippy -D warnings, el compose renderiza, cada variable de
              # configuración en .env.example, tests de los hooks del plugin, slash commands iguales a recipes/
make test     # suite E2E contra un Postgres 18 y un NATS 2.12 JetStream desechables (necesita docker)
make up-dev   # stack local construido desde este checkout
make help     # todo lo demás
```

O a mano: un Postgres local (`docker run -d -p 5432:5432 -e
POSTGRES_PASSWORD=bus -e POSTGRES_USER=bus -e POSTGRES_DB=bus
postgres:18-alpine`), `export DATABASE_URL=postgres://bus:bus@localhost:5432/bus`,
después `cargo run -- serve` (migra al arrancar). Los tests necesitan además
el fixture de JetStream; el fichero de configuración es obligatorio porque
sube `max_payload`:

```bash
docker run -d -p 4222:4222 -v "$PWD/Docker/nats-test.conf:/etc/nats/nats.conf:ro" \
    nats:2.12-alpine -js -c /etc/nats/nats.conf
TEST_DATABASE_URL=$DATABASE_URL TEST_NATS_URL=nats://127.0.0.1:4222 cargo test
```

### Política de toolchain

El MSRV del crate es el `rust-version` de `Cargo.toml` (**1.98.1**). CI lo
comprueba en cada push: un job con la stable actual (formato, Clippy, tests)
y otro que compila y testea con el MSRV fijado, así una dependencia que
exija un compilador más nuevo falla antes de publicar y no en tu
`cargo install`.

Subir el MSRV es un cambio deliberado: en el mismo PR se cambian
`rust-version` en `Cargo.toml`, cada pin del job de MSRV de
`.github/workflows/ci.yml`, la imagen del builder en `Docker/Dockerfile` y
este párrafo tanto en `README.md` como en `README.es.md`, y se explica el
motivo en las notas de la release.

El MSRV es alto a propósito, y tiene un coste que conviene decir: compilar
desde fuente con `cargo install` exige un compilador al menos así de nuevo,
así que las distribuciones con un Rust más viejo no pueden. La imagen de
contenedor y los binarios precompilados no se ven afectados — ninguno compila
nada en tu máquina.

La imagen Docker se compila con esa misma versión, sobre Alpine, así que el
binario queda enlazado estáticamente contra musl. Eso es lo que libera al
stage de runtime de tener que seguir la distribución del builder — el
emparejamiento que rompió v0.4.0, donde un binario glibc se encontró con un
runtime de glibc más antigua y la imagen no arrancaba.

Las releases con tag pasan el gate completo de CI, después arrancan la imagen
recién construida contra un Postgres real y hacen una llamada MCP
autenticada, y solo entonces publican la imagen multi-arch.

## Estructura

```
src/
  main.rs        CLI (serve / migrate / team / agent / token / webhook /
                 conversations / admin / context / mcp / client / recipes / *-config)
  serve.rs       axum + transporte MCP Streamable HTTP + auth middleware
  auth.rs        tokens bearer -> AuthCtx (agente + equipo + sesión)
  context.rs     resolver local: perfiles, .acs.toml, binding de host-session
  proxy.rs       `mcp proxy` — un servidor MCP stdio por conversación
  spool.rs       el spool de referencias de inbox del proxy, con fsync
  hook.rs        `context hook` — lo que llama un hook autenticado
  tools/         capa MCP (una tool por operación, tipadas con schemars)
  store/         toda la lógica y todo el SQL; backend.rs + routing.rs eligen
                 dónde vive el cuerpo de una conversación
  events.rs      hub LISTEN/NOTIFY sobre bus_events (self-ping, reenganche)
  webhooks.rs    despachador de webhooks salientes
  dashboard/     /dashboard read-only (un token se intercambia una vez por una cookie firmada)
  ratelimit.rs   token bucket por credencial
  admin.rs       comandos de operador junto a Postgres
  admin_api.rs   API REST /admin/* (credenciales acsa_, nunca MCP)
  admin_cli.rs   `ai-crew-sync admin …` contra un bus remoto
  client/        cliente de consola (`ai-crew-sync client …`)
  recipes.rs     embebe recipes/
migrations/      esquema sqlx (se aplica solo al arrancar)
recipes/         16 procedimientos independientes del host: el origen de plugin/commands
                 (`make recipes`; si se apartan, falla `make check`) y de `ai-crew-sync recipes`
plugin/          plugin de Claude Code (MCP + hooks + comandos + skill)
  .claude-plugin/plugin.json
  .mcp.json      arranca `ai-crew-sync mcp proxy`; no lleva credencial
  hooks/         SessionStart (catch-up + heartbeat), Stop y SessionEnd
  scripts/       bus-call.sh, heartbeat.sh, session-start.sh, stop-drain.sh
                 (autenticados por el binario, por perfiles locales, o curl + python3)
  commands/      generados desde recipes/: /ai-crew-sync:standup|catchup|announce|ask|
                 claim|done|handoff|board|who|lock|unlock|note|wait|thread|inbox|review
  skills/        convenciones de coordinación
tests/           la suite de integración (servidor real, Postgres y NATS)
examples/        .mcp.json, .mcp.http.json, codex-config.toml, snippet de CLAUDE.md
packaging/       unidad systemd y fichero de entorno para el .deb/.rpm
docs/            ADRs, guías de operación, scripts de aceptación
Docker/          Dockerfile + el único compose (imagen publicada, build local, apto Swarm)
Makefile         check / test / up / up-dev / deploy — `make help` lista todo
.claude-plugin/marketplace.json   este repo funciona como marketplace
```

## Límites

Acotados para que un agente descontrolado no agote el bus. Cada rechazo
nombra el límite y qué hacer en su lugar, porque quien llama es un modelo.

| Límite | Default | Knob |
|---|---|---|
| Cuerpo de petición MCP | 8 MiB (413) | `BUS_MAX_REQUEST_BYTES` |
| Peticiones por credencial presentada (un token de agente, o la credencial de sesión de cada ventana) | 600/min por proceso (429 + `Retry-After`); 0 lo desactiva | `BUS_RATE_LIMIT_PER_MINUTE` |
| Cuerpo de petición `/admin` | 16 KiB (413) | — |
| Cuerpo de mensaje (canales, DMs, conversaciones), valor de nota | 1 MiB | — |
| Título de conversación, nombre de proyecto | 200 B | — |
| Miembros por conversación | 200 | — |
| Adjunto | 256 KiB, 8 por mensaje/tarea | — |
| Objeto `metadata` | 16 KiB | — |
| Título / descripción / resultado de tarea | 512 B / 64 KiB / 64 KiB | — |
| Dependencias de una tarea | 32 | — |
| Ámbito / clave de nota | 64 B / 256 B | — |
| Tags de nota | 16 tags, 64 B cada uno | — |
| Topic de canal, campos de presencia | 256 B | — |

El rate limiting es **por proceso**: el servidor es stateless por diseño, así
que con N réplicas el techo efectivo es N × el límite. Es deliberado — un
limitador compartido exigiría estado compartido en cada petición. Pon el
límite global duro en el proxy inverso y deja este como red de seguridad de
la instancia con la que el agente habla.

Ajustes recomendados de proxy al exponer el bus: limita el cuerpo al mismo
valor (`client_max_body_size 8m` en nginx, `request_body_limit` en Caddy),
limita `/health` y `/dashboard` aparte (no los cubre el limitador por
token — `/health` no lleva token), y mantén los timeouts de lectura por
encima de 60s para que `wait_for_updates` y `ask_agent` (ambos con un tope
de 55s) no se corten a mitad de la espera — y por encima de 300s en un equipo
con las conversaciones encendidas, porque `wait_for_conversation_updates`
puede bloquearse ese tiempo.

## Capacidad y retención

Los adjuntos se guardan en Postgres, así que la base de datos es el almacén
de objetos — dimensiona su disco en consecuencia. Las cuotas son opt-in por
equipo e ilimitadas por defecto:

```bash
ai-crew-sync team quota --team acme --bytes 1073741824   # 1 GiB de adjuntos
ai-crew-sync team quota --team acme                      # quitarla
ai-crew-sync team usage --team acme                      # cuentas y bytes, nunca contenido
ai-crew-sync team prune --team acme --older-than-days 90  # dry run: solo informa
ai-crew-sync team prune --team acme --older-than-days 90 --apply
```

`usage` avisa al 80%. Una subida que cruzaría la cuota se rechaza con un
error accionable y no deja nada a medias — la comprobación y el INSERT
comparten transacción, así que dos subidas simultáneas no pueden ocupar
ambas el último hueco.

`prune` recorta **historial**: mensajes de canal y directos (y los adjuntos
que cuelgan de ellos), revisiones de notas y eventos de tareas más antiguos
que la ventana. Los hilos de conversación, sus receipts y su auditoría no se
purgan. Las notas y las tareas nunca se purgan — son la memoria durable del
equipo, y solo se recorta el historial de detrás. Es dry run salvo que pases
`--apply`, y los números del dry run son los de verdad: ejecuta los DELETE en
una transacción y hace rollback.

Respalda el volumen de Postgres como el sistema de registro que es; no hay
una segunda copia de un adjunto en ningún sitio. En un equipo enrutado a
JetStream, los cuerpos de conversación salen de Postgres en cuanto el broker
los confirma, así que respalda los dos juntos (`pg_dump` más
`nats stream backup` de los streams del equipo); el simulacro de
restauración está en `docs/operations/jetstream.md`.

## Seguridad

- Sirve siempre detrás de TLS (Caddy/nginx/Traefik) si sale de tu red.
- `BUS_ALLOWED_HOSTS` valida el header `Host` (anti DNS-rebinding); ponlo a tu
  hostname real o usa `*` solo detrás de un proxy que ya lo valide. El binario
  trae por defecto nombres de localhost; el fichero compose, `.env.example` y
  el fichero de entorno de los paquetes traen `*` por defecto.
  `BUS_ALLOWED_ORIGINS` restringe qué orígenes de navegador pueden llamar a
  `/mcp` (vacío por defecto, lo que desactiva la comprobación).
- Tres clases de credencial, tres formas de acabar con ellas. Tokens de
  agente: `token revoke --id` junto a Postgres o
  `admin token revoke --team T --id ID` en remoto; revocar un token acaba
  también con todas las credenciales de sesión derivadas de él. Una ventana:
  `revoke_session`. Credenciales administrativas:
  `admin credential revoke --id ID` (`admin logout` solo olvida la copia
  local). Deshabilita el agente de una persona con `agent disable` (solo junto
  a Postgres).
- Los mensajes directos solo los ven sus dos partes: el agente que lo envía y
  el que lo recibe (un mensaje a una ventana se enruta allí, pero cualquier
  ventana del destinatario puede leerlo con `all_sessions`). Las
  conversaciones solo las ven sus miembros, y los hilos de proyecto solo los
  agentes con una concesión sobre el proyecto. Canales, tareas, notas y
  presencia son visibles para todo el equipo (ese es el punto).

## Decisiones de arquitectura

`docs/adr/0001-authenticated-sessions-and-staged-messaging.md` recoge la
dirección aceptada, y sus siete fases están ya implementadas: credenciales de
sesión emitidas por el servidor y derivadas de un token de agente, hooks de
ciclo de vida apoyados en el binario `ai-crew-sync`, conversaciones y receipts
por destinatario sobre Postgres, y una ruta opcional por JetStream para
cuerpos de conversación y fanout de inbox.

Implementado no es lo mismo que activo. Las conversaciones están apagadas
hasta que un operador las enciende para un equipo
(`team capability --conversations on`), JetStream está apagado hasta que un
operador provisiona los streams del equipo, arranca el servidor con
`--nats-url` y enruta a él el equipo (con las conversaciones encendidas), y
una instalación por defecto no contacta con ningún broker. Instalar una
release no enciende nada por sí solo. El comportamiento anterior (todo el
estado en Postgres, `X-Crew-Session` como etiqueta que envía quien llama, y
hooks que solo necesitan `curl` y `python3`) sigue funcionando en todo
momento, y es lo que obtiene un cliente sin binario y con `BUS_TOKEN`
exportado.

## Contribuir y contacto

¡Las contribuciones son bienvenidas! Si quieres contribuir:

1. Haz fork del repositorio.
2. Crea una rama para tu feature o corrección.
3. Haz tus cambios y comprueba que el proyecto compila y los tests pasan (`make check && make test`).
4. Commitea y sube tu rama a tu fork.
5. Abre un pull request contra el repositorio principal.

Para dudas, problemas o feedback, contacta con el mantenedor:

### **Contacto**

- **Autor**: Joaquín Béjar García
- **Email**: <jb@taunais.com>
- **Telegram**: [@joaquin_bejar](https://t.me/joaquin_bejar)
- **Repositorio**: <https://github.com/joaquinbejar/ai-crew-sync>
- **Crate**: <https://crates.io/crates/ai-crew-sync>
- **Documentación**: <https://docs.rs/ai-crew-sync>

¡Gracias por tu interés!

**Licencia**: MIT

<!-- related-projects:start -->

## Proyectos relacionados

Repositorios del mismo autor de los que depende este proyecto, y repositorios que dependen de él.

### Usado por

| Repository | Description |
|------------|-------------|
| [homebrew-tap](https://github.com/joaquinbejar/homebrew-tap) | Homebrew formulae for joaquinbejar's tools. *(Homebrew formula)* |

<!-- related-projects:end -->
