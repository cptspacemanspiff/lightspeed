# GitHub App sub-leases

**Status:** Agreed 2026-10-19; first slice built 2026-10-20 (sub-leases,
bots holding them for MCP servers). The environment lease for agent-gitd is
designed here and not built.

A GitHub App's key is the most powerful GitHub credential a deployment holds:
whoever has it can mint tokens for every repo the App is installed on, with
every permission it has. Lightspeed should be the only holder, and everything
else should get short-lived tokens narrowed to what it needs. The first user
is the ExamGrader feedback triage bot, which files issues in one repo; the
next is agent-gitd, the git and forge broker for agents in environments,
which pushes branches and opens pull requests for the session it serves.

## Agreed

- **Lightspeed holds the App key; nothing else does.** It already stores the
  key encrypted (`auth/providers/create`) and mints installation tokens on
  demand (`crates/auth/src/github.rs`). agent-gitd leases tokens from
  Lightspeed instead of keeping a copy. Two holders of a key that can push
  code is the case to avoid.
- **A sub-lease is a narrowed App grant.** It names a parent grant (the
  installation, or another sub-lease), a list of repositories and a set of
  permissions. Every token minted for it sends that list and set to
  `POST /app/installations/{id}/access_tokens`, so GitHub enforces the
  narrowing, not only Lightspeed.
  - The repository list is required. GitHub would read an absent list as
    every repo of the installation, which widens silently as the App is
    installed on more repos.
  - Permissions only narrow: each must be at most the parent's level (read <
    write < admin). Creating a sub-lease mints one token with its scope, so
    a repo the installation doesn't cover fails at creation, not at first
    use.
  - A sub-lease is usable only while its whole parent chain is active:
    revoking the installation grant stops its sub-leases.
  - A repo is the finest scope GitHub offers. Branch limits stay with
    agent-gitd (`agents/<id>/**`) and a GitHub ruleset.
- **Sub-leases attach to sessions.** The same narrowed permissions apply
  whichever way a session reaches GitHub:
  - its own MCP calls (no environment): the worker sends a token minted for
    the session's sub-lease as the MCP server's bearer;
  - agent-gitd, acting for an agent in the session's environment: it asks
    Lightspeed for a token for that environment, and Lightspeed maps
    environment → session → sub-lease (designed, not built; see below).
- **A bot is where its sessions get sub-leases.** The bot record's
  `mcpCredentials` maps an MCP server id to a sub-lease (or a static bearer
  grant). Its sessions use that credential for that server instead of the
  server's own. Setting it needs the authority to bind credentials to MCP
  servers (`mcp/servers/put`), not only `bots/put`.
- **One invariant changes.** An MCP server's credential used to belong to
  the universe alone ("sessions select only `server_id` and never override
  this binding"). Now a bot can supply it for its own sessions. The session,
  its program and its model still can't.
- **Tokens renew on use.** The broker caches a minted token in the worker's
  memory until a minute before it expires and mints the next on the next
  call. Nothing renews on a timer, and no token is stored.

## How the first slice works

- `auth/grants/sublease` creates a sub-lease: `parentGrantId`,
  `repositories`, `permissions`, optional `audience`, `exposure`,
  `displayName` and `grantId`. The sub-lease is a `github_app` grant whose
  metadata adds `parent_grant_id` and `token_scope`. Its audience defaults to
  the parent's; a sub-lease meant for GitHub's MCP server names
  `https://api.githubcopilot.com/mcp/`.
- A session belongs to a bot when its id is one of the bot's
  (`bot:v1:{bot}…`). Only the runtime may create such ids, so the mapping
  can't be forged. An id two bots could both claim (`bot:v1:x-g2` is bot
  `x`'s second generation and bot `x-g2`'s first) gets neither bot's
  credentials.
- Session preparation reads the bot's `mcpCredentials` and gives the MCP
  tool spec an `auth_grant` credential reference instead of the server's
  `mcp_server` one. A bot's change applies to its sessions from their next
  preparation; revoking the grant applies at the next call.
- The worker resolves an `auth_grant` reference through the broker with the
  server URL as the audience, so the grant's audience must cover the server.
  Its tool-list cache is keyed by credential too, since two credentials can
  see different tools.

## Later

- **The environment lease for agent-gitd.** A trusted-service call such as
  "token for environment E, service github" that Lightspeed answers by
  mapping the environment to the session it serves and minting for that
  session's sub-lease. agent-gitd learns the environment from the
  provider's stamp on the connection, not from the agent, so it can't
  obtain more than the session holds. This waits for agent-gitd's
  connection identity, which isn't agreed.
- **Sub-leases on sessions that aren't a bot's**, set by whoever starts the
  session with the authority above.
- **Web UI**: sub-leases on the Credentials page, and the bot form's MCP
  credentials.
