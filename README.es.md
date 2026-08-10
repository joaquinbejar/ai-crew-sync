# ai-crew-sync

[![CI](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml/badge.svg)](https://github.com/joaquinbejar/ai-crew-sync/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/ai-crew-sync.svg)](https://crates.io/crates/ai-crew-sync)
[![docs.rs](https://docs.rs/ai-crew-sync/badge.svg)](https://docs.rs/ai-crew-sync)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

*Read this in [English](README.md).*

Servidor MCP en Rust que hace de **bus de coordinación entre los agentes de
código IA de un equipo** — Claude Code, Codex, Cursor, Kimi o cualquier otro
que hable MCP sobre Streamable HTTP — con todo el estado en Postgres. Cada
agente (el tuyo, el de cada compañero) se conecta con su propio token y puede:

| Capacidad | Herramientas MCP |
|---|---|
| Mensajería (canales + directos, cursores de lectura, búsqueda) | `post_message`, `read_messages`, `search_messages`, `list_channels`, `create_channel` |
| Coordinación de tareas con leases y **dependencias** (`depends_on`) | `create_task`, `claim_task`, `claim_next_task`, `renew_task_lease`, `release_task`, `complete_task`, `list_tasks`, `get_task` |
| **Tiempo real**: bloquearse hasta que pase algo relevante (LISTEN/NOTIFY) | `wait_for_updates` |
| **RPC agente↔agente**: preguntar a un compañero y esperar su respuesta en una llamada | `ask_agent` |
| **Adjuntos**: diffs, logs, archivos pequeños (≤256 KiB) en mensajes y tareas | `attach_file`, `get_attachment` (+ `attachments` en `post_message`) |
| **Locks genéricos** con TTL sobre recursos ("deploy:staging") | `acquire_lock`, `release_lock`, `list_locks` |
| Presencia (quién está en qué repo/rama haciendo qué), con las sesiones abiertas de cada compañero bajo su nombre | `heartbeat`, `list_agents` |
| Memoria compartida del equipo (notas con historial) | `set_note`, `get_note`, `list_notes`, `search_notes`, `delete_note` |
| **Resumen de actividad** de las últimas N horas | `team_digest` |
| **Sesiones**: un token, un contexto de trabajo por repo | cabecera `X-Crew-Session` (abajo) |
| **Anuncios** que llegan a todas las sesiones estén en lo que estén | `announce` en `post_message` |
| Identidad | `whoami` |

Decisiones de diseño:

- **La identidad sale del token**, nunca de un argumento: un agente no puede
  hablar en nombre de otro.
- **Multi-equipo**: todo está aislado por `team`; un despliegue sirve para
  varios squads.
- **Stateless**: transporte MCP Streamable HTTP sin sesiones, así que escala
  horizontal detrás de cualquier balanceador.
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

Un solo binario es el servidor, el CLI de operador y el cliente de consola.
El `.deb` y el `.rpm` instalan además una unidad systemd endurecida y un
fichero de entorno legible solo por root en
`/etc/ai-crew-sync/ai-crew-sync.env`. El servicio queda **deshabilitado**,
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
make up      # = docker compose -f Docker/docker-compose.yml up -d (imagen de GHCR)
```

Todas las variables tienen default razonable; se sobrescriben por entorno o
en `./.env` (parte de `.env.example`, que documenta cada knob con su
default — pon un `POSTGRES_PASSWORD` real para cualquier cosa no local). `make up-dev` construye desde el checkout.
**Docker Swarm** funciona con el mismo fichero:

```bash
export POSTGRES_PASSWORD=...   # Swarm no lee ficheros .env
docker stack deploy -c Docker/docker-compose.yml crew   # o: make deploy
```

El bus es stateless — escala réplicas de `bus` sin más tras el routing mesh.

El servidor migra la base de datos al arrancar y expone:

- `POST /mcp` — endpoint MCP (requiere `Authorization: Bearer acs_...`)
- `GET /health` — para el balanceador
- `GET /dashboard` — panel read-only para humanos (presencia, tareas, locks,
  últimos mensajes de canal; los DMs nunca aparecen). Se refresca solo cada
  15s. Ábrelo en el navegador y pega un token de agente una vez: se
  intercambia por una cookie de sesión HttpOnly, de vida corta y de solo
  lectura, que **no puede llamar a herramientas MCP**. Los scripts se saltan
  el intercambio y mandan `Authorization: Bearer acs_...` directamente. El
  token nunca se acepta en la query string — una URL acaba en el historial,
  en los referrers y en los logs del proxy.

### Desplegar en producción

El compose base trae un default que funciona para todo, para que `make up`
arranque en un portátil. Producción usa un overlay que **no** tiene defaults:

```bash
export POSTGRES_PASSWORD=…        # no el valor de ejemplo
export BUS_VERSION=0.4.1          # inmutable, nunca `latest`
export BUS_ALLOWED_HOSTS=bus.tu-empresa.com
export BUS_DASHBOARD_SECRET=…     # compartido, para que la sesión valga en cualquier réplica
make deploy                       # preflight y después docker stack deploy
```

`make deploy` se niega antes de tocar el clúster si falta alguno, si sigue la
contraseña de ejemplo o si el tag es móvil — y el propio compose ni siquiera
renderiza el overlay sin ellos. `make deploy-check` ejecuta solo el preflight.

## Dar de alta al equipo

```bash
export DATABASE_URL=postgres://bus:...@localhost:5432/bus

ai-crew-sync team create --slug acme --name "Acme Squad"
ai-crew-sync agent add --team acme --name joaquin     # imprime su token
ai-crew-sync agent add --team acme --name marta
```

El token se enseña **una sola vez**.

Gestión posterior: `agent list`, `agent disable`, `token issue`, `token list`,
`token revoke`.

### Un agente por herramienta, no por persona

Si usas Claude Code *y* Codex —o dos agentes de código cualesquiera— dale a
cada uno su propio agente:

```bash
ai-crew-sync agent add --team acme --name joaquin        --with-token   # Claude Code
ai-crew-sync agent add --team acme --name joaquin-codex  --with-token   # Codex
```

Compartir un token entre dos herramientas las convierte en **el mismo agente**
para el bus, y la coordinación deja de funcionar entre ellas en silencio: las
dos reclaman la misma tarea y a las dos se les dice que la tienen, una suelta
el lock de la otra, sus heartbeats se pisan, y leer en una marca como leídos
los mensajes de la otra. Nada da error — son indistinguibles, así que no hay
nada que rechazar.

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

Llamar a un canal como un repo es lo que hace funcionar el canal por defecto de
la sesión — ver [Sesiones](#sesiones-una-persona-varios-repos) más abajo.

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
ejecuta, dentro de Claude Code:

```
/plugin marketplace add tu-org/ai-crew-sync
/plugin install ai-crew-sync@ai-crew-sync
```

y exporta en su shell (p. ej. `~/.zshrc`):

```bash
export BUS_URL=https://bus.tu-empresa.com/mcp
export BUS_TOKEN=acs_...   # su token personal, de `ai-crew-sync agent add`
```

El plugin trae todo preconfigurado:

- **MCP** `ai-crew-sync` apuntando a `$BUS_URL` con su `$BUS_TOKEN` (sin tocar JSON a mano).
- **Hooks**: al arrancar una sesión hace heartbeat y le inyecta a Claude un
  resumen del equipo (DMs sin leer, tareas propias, `team_digest` de las últimas
  8 h — configurable con `BUS_DIGEST_HOURS`); tras cada respuesta renueva la
  presencia con el repo/rama del checkout, y al cerrar sesión marca `idle`.
  Si `BUS_URL`/`BUS_TOKEN` no están definidos, los hooks no hacen nada.

Pon `BUS_SESSION` por repo para que cada ventana sea su propio contexto de
trabajo — `direnv` es la forma limpia, porque Claude Code lee el entorno al
arrancar:

```bash
# .envrc en cada repo (ignóralo en git si además guarda un token)
export BUS_SESSION=market-data
```

Con eso el plugin etiqueta la sesión, la presencia enseña el repo correcto de
cada ventana, y los claims y locks dejan de chocar entre ellas. Sin eso todo
sigue funcionando igual que antes: la cabecera lleva un fallback `:-`, así que
una variable sin definir manda vacío y no el literal `${BUS_SESSION}`.

El hook `Stop` además drena preguntas: cuando el agente de un compañero está
bloqueado en `ask_agent`, la sesión se mantiene abierta lo justo para
contestar antes de callarse — primero la pregunta que lleva más tiempo
esperando, una por turno, y nunca una que ya hayas respondido. **Esto no hace contestable
una sesión parada**: un agente de código solo llama a herramientas mientras
procesa un turno, así que una ventana parada una hora en el prompt no contesta
hasta que su humano escriba. Es una propiedad del cliente, no del bus; para lo
que no pueda esperar, usa una tarea o un mensaje de canal.

- **Comandos**: `/ai-crew-sync:standup [horas]`, `/ai-crew-sync:catchup [horas]`,
  `/ai-crew-sync:announce [#canal] mensaje` y `/ai-crew-sync:ask <agente> <pregunta>`.
- **Skill** con las convenciones (reclamar antes de trabajar, locks para
  deploys, `wait_for_updates` para esperar respuestas), que Claude carga solo
  cuando toca coordinarse.

Los hooks solo necesitan `curl` y `python3` en el PATH.

### Opción B (cualquier cliente MCP): configuración manual

Entrada MCP estándar — en Claude Code va en `~/.claude.json` (ámbito usuario)
o en un `.mcp.json` commiteado en la raíz del repo (ver `examples/.mcp.json`);
en Cursor, Codex, Kimi o cualquier otro agente con MCP, su fichero de
configuración equivalente. El token se lee de una variable de entorno:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://bus.tu-empresa.com/mcp",
      "headers": { "Authorization": "Bearer ${TEAM_BUS_TOKEN}" }
    }
  }
}
```

También puedes generar el bloque con:

```bash
ai-crew-sync mcp-config --url https://bus.tu-empresa.com/mcp --token acs_...
```

Con eso, cada agente ve las herramientas del bus y las usa solo. Para que las
use *bien*, añade las convenciones del equipo al fichero de instrucciones del
repo (`CLAUDE.md`, `AGENTS.md` o equivalente) — hay un snippet listo en
`examples/CLAUDE.md-snippet.md`.

### Sesiones: una persona, varios repos

Un token identifica a una **persona**, y una persona suele tener varias
sesiones de código abiertas a la vez — normalmente una por repo. Añade la
cabecera `X-Crew-Session` para que cada una tenga su propio contexto de
trabajo:

```json
{
  "mcpServers": {
    "ai-crew-sync": {
      "type": "http",
      "url": "https://bus.tu-empresa.com/mcp",
      "headers": {
        "Authorization": "Bearer ${TEAM_BUS_TOKEN}",
        "X-Crew-Session": "market-data"
      }
    }
  }
}
```

La etiqueta es libre, hasta 64 bytes, y se normaliza como un nombre de canal
(sin espacios sobrantes y en minúsculas, para que `Market-Data` y
`market-data` sean una sola sesión y no dos que no se ven entre sí). El nombre
del repo es la elección natural.

Una sesión **no** es identidad. Llega en una cabecera y no en el token, así
que nunca puede hacerte hablar por otro; solo separa tu presencia, tus claims
y tus locks de tus otras sesiones. Si omites la cabecera tienes la sesión
compartida, que es exactamente como se comportaba el bus antes de que las
sesiones existieran.

El cliente de consola acepta `--session` (o `BUS_SESSION`), y
`ai-crew-sync mcp-config --session market-data` mete la cabecera en el bloque
generado.

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
incluida otra sesión tuya.

Los DM pueden dirigirse a una **sesión**, no solo a una persona:

| `to` | Llega a |
|---|---|
| `dani` | la persona — todas las sesiones que tenga abiertas |
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

Llama a un canal como una sesión y pasa a ser su canal por defecto: sin
`channel` ni `to`, `post_message` va ahí, `team_digest` lo resume, y
`wait_for_updates` deja de despertarse con el ruido de los canales de otros
repos. Los DM, tareas, locks y notas te despiertan siempre — silenciarlos
escondería trabajo, no ruido. `all_channels: true` vuelve a abarcar todo el
equipo en cualquiera de las dos llamadas, y un `channel` explícito siempre
manda.

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

**El servidor.** Las migraciones son aditivas por norma, así que un binario
nuevo lee una base que escribió uno viejo y al revés. Eso es lo que hace
seguro un reinicio rodante y superable una vuelta atrás.

```bash
export BUS_VERSION=0.6.0
make deploy                                    # Swarm; o:
docker compose -f Docker/docker-compose.yml pull && \
  docker compose -f Docker/docker-compose.yml up -d
```

El contenedor migra al arrancar, y el servicio empaquetado también — los dos
traen `BUS_AUTO_MIGRATE=true` — así que actualizar por `.deb` o `.rpm` es el
paquete más un reinicio:

```bash
sudo dpkg -i ai-crew-sync_amd64.deb            # o: sudo rpm -U ai-crew-sync.x86_64.rpm
sudo systemctl restart ai-crew-sync
```

Si lo has desactivado y migras a propósito, hazlo como **root**:
`DATABASE_URL` vive en `/etc/ai-crew-sync/ai-crew-sync.env`, que systemd carga
para la unidad y que solo puede leer root, así que `sudo -u ai-crew-sync`
arranca el binario sin ella.

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

**El plugin de Claude Code.** Los marketplaces de terceros tienen la
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

**Los demás clientes MCP** —Codex, Cursor, Zed, un script— no tienen nada que
actualizar. Las herramientas viven en el servidor, así que una herramienta o un
argumento nuevos aparecen la próxima vez que el cliente reconecta. Las
**cabeceras** nuevas, como `X-Crew-Session`, son la excepción: esas viven en la
configuración del cliente y hay que añadirlas a mano.

## Cliente de consola

El mismo binario habla con el bus desde la terminal, como un agente más — útil
para humanos, scripts y CI:

```bash
export BUS_URL=https://bus.tu-empresa.com/mcp
export BUS_TOKEN=acs_...

ai-crew-sync client whoami
ai-crew-sync client send --channel deploys --body "staging lleva la 1.4.2"
ai-crew-sync client send --to marta --body "mira el PR 421"
ai-crew-sync client read --scope inbox
ai-crew-sync client agents
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

Todos los subcomandos aceptan `--json` para salida cruda (pipeable a `jq`).

## Webhooks salientes (puente a humanos)

El bus puede avisar a Slack/Discord (o a cualquier endpoint JSON) cuando pasan
cosas: mensaje en canal, tarea que cambia de estado, lock adquirido/liberado,
nota actualizada. **Los mensajes directos nunca se reenvían.**

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

## Desarrollo

```bash
make check    # gate pre-push: rustfmt, clippy -D warnings, compose renderiza
make test     # suite E2E contra un Postgres 18 desechable (necesita docker)
make up-dev   # stack local construido desde este checkout
make help     # todo lo demás
```

O a mano: un Postgres local (`docker run -d -p 5432:5432 -e
POSTGRES_PASSWORD=bus -e POSTGRES_USER=bus -e POSTGRES_DB=bus
postgres:18-alpine`), `export DATABASE_URL=postgres://bus:bus@localhost:5432/bus`,
después `cargo run -- serve` (migra al arrancar) y
`TEST_DATABASE_URL=$DATABASE_URL cargo test`.

### Política de toolchain

El MSRV del crate es el `rust-version` de `Cargo.toml` (**1.97.1**). CI lo
comprueba en cada push: un job con la stable actual (formato, Clippy, tests)
y otro que compila y testea con el MSRV fijado, así una dependencia que
exija un compilador más nuevo falla antes de publicar y no en tu
`cargo install`.

Subir el MSRV es un cambio deliberado: en el mismo PR se cambian
`rust-version`, el pin de `.github/workflows/ci.yml` y este párrafo, y se
explica el motivo en las notas de la release.

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
  main.rs        CLI (serve / migrate / team / agent / token / client / mcp-config)
  serve.rs       axum + transporte MCP Streamable HTTP + auth middleware
  auth.rs        tokens bearer -> AuthCtx (agente + equipo)
  tools/         capa MCP (una tool por operación, tipadas con schemars)
  store/         toda la lógica y todo el SQL
  admin.rs       comandos de operador
  client.rs      cliente de consola
migrations/      esquema sqlx (se aplica solo al arrancar)
plugin/          plugin de Claude Code (MCP + hooks + comandos + skill)
  .claude-plugin/plugin.json
  .mcp.json      servidor MCP parametrizado con BUS_URL/BUS_TOKEN
  hooks/         SessionStart (catch-up + heartbeat), Stop y SessionEnd
  scripts/       bus-call.sh, heartbeat.sh, session-start.sh (curl + python3)
  commands/      /ai-crew-sync:standup|catchup|announce|ask
  skills/        convenciones de coordinación
Docker/          Dockerfile + compose (imagen publicada, apto Swarm) + override dev
Makefile         check / test / up / up-dev / deploy — `make help` lista todo
.claude-plugin/marketplace.json   este repo funciona como marketplace
```

## Límites

Acotados para que un agente descontrolado no agote el bus. Cada rechazo
nombra el límite y qué hacer en su lugar, porque quien llama es un modelo.

| Límite | Default | Knob |
|---|---|---|
| Cuerpo de petición MCP | 8 MiB (413) | `BUS_MAX_REQUEST_BYTES` |
| Peticiones por token | 600/min, en proceso (429 + `Retry-After`) | `BUS_RATE_LIMIT_PER_MINUTE` |
| Cuerpo de mensaje, valor de nota | 1 MiB | — |
| Adjunto | 256 KiB, 8 por mensaje/tarea | — |
| Objeto `metadata` | 16 KiB | — |
| Título / descripción / resultado de tarea | 512 B / 64 KiB / 64 KiB | — |
| Dependencias de una tarea | 32 | — |
| Tags de nota | 16 tags, 64 B cada uno | — |
| Topic de canal, campos de presencia | 256 B | — |

El rate limiting es **por proceso**: el servidor es stateless por diseño, así
que con N réplicas el techo efectivo es N × el límite. Es deliberado — un
limitador compartido exigiría estado compartido en cada petición. Pon el
límite global duro en el proxy inverso y deja este como red de seguridad de
la instancia con la que el agente habla.

Ajustes recomendados de proxy al exponer el bus: limita el cuerpo al mismo
valor (`client_max_body_size 8m` en nginx), limita `/health` y `/dashboard`
aparte (no los cubre el limitador por token — `/health` no lleva token), y
mantén los timeouts de lectura por encima de 60s para no cortar los
long-polls de `wait_for_updates` y `ask_agent`.

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

`prune` recorta **historial**: mensajes (y los adjuntos que cuelgan de
ellos), revisiones de notas y eventos de tareas más antiguos que la ventana.
Las notas y las tareas nunca se purgan — son la memoria durable del equipo, y
solo se recorta el historial de detrás. Es dry run salvo que pases `--apply`,
y los números del dry run son los de verdad: ejecuta los DELETE en una
transacción y hace rollback.

Respalda el volumen de Postgres como el sistema de registro que es; no hay
una segunda copia de un adjunto en ningún sitio.

## Seguridad

- Sirve siempre detrás de TLS (Caddy/nginx/Traefik) si sale de tu red.
- `BUS_ALLOWED_HOSTS` valida el header `Host` (anti DNS-rebinding); ponlo a tu
  hostname real o déjalo en `*` solo detrás de un proxy que ya lo valide.
- Revoca tokens con `token revoke`; deshabilita personas con `agent disable`.
- Los mensajes directos solo los ve el destinatario; canales, tareas, notas y
  presencia son visibles para todo el equipo (ese es el punto).

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
