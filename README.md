# energy-atlas-ai

Energy Atlas AI is a search-first assistant for exploring and analyzing U.S. energy data from EIA, GridStatus, Dallas Fed, CFTC, and official outlook sources.

## System dependencies

Install these first:

- Python 3.12
- Poetry
- Docker
- Docker Compose
- Stripe CLI

Optional but useful:

- PostgreSQL client tools
- `make`

## Environment

Create a `.env` file in the repo root. At minimum, local app development typically needs:

```env
DJANGO_CONFIGURATION=Development
DJANGO_DEBUG=true
DJANGO_SECRET_KEY=replace-me

DATABASE_URL=sqlite:///app/alerts.sqlite3

STRIPE_TEST_SECRET_KEY=sk_test_...
STRIPE_LIVE_MODE=false
```

If you run Stripe webhooks locally with the Stripe CLI, you may also want:

```env
STRIPE_WEBHOOK_SECRET=whsec_...
```

For production deployments, keep a separate `.env.production` file on your local machine and do not commit it. The deployment scripts can upload that file to the server as the remote `.env`.

## Install Python dependencies

```bash
poetry install
```

## Run Chainlit locally

From the repo root:

```bash
poetry run chainlit run chainlit/app.py -w
```

Notes:

- Chainlit config/browser changes may appear cached in the browser.
- The default local Chainlit app file is [chainlit/app.py](/Users/mozilla/Documents/projects/energy-atlas-ai/chainlit/app.py).

## Run the Django app locally

From the repo root:

```bash
poetry run python app/manage.py migrate
poetry run python app/manage.py runserver
```

The Django app lives under [app/](/Users/mozilla/Documents/projects/energy-atlas-ai/app).

Important:

- Django loads `.env` from the repo root and `app/.env`.
- The pricing page Stripe checkout flow expects `STRIPE_TEST_SECRET_KEY` and an active `PlanPrice` row for the local `pro` plan.

## Run the Django app locally with Stripe webhook forwarding

1. Start Django:

```bash
poetry run python app/manage.py runserver
```

2. In a second terminal, start Stripe CLI forwarding:

```bash
stripe listen --forward-to localhost:8000/stripe/webhook/
```

This repo also accepts:

```bash
stripe listen --forward-to localhost:8000/billing/webhook/
```

That compatibility route points to the same local subscription-sync handler.

3. Stripe CLI will print a signing secret like:

```text
whsec_...
```

4. If you verify webhook signatures yourself later, add it to `.env`:

```env
STRIPE_WEBHOOK_SECRET=whsec_...
```

5. Trigger test events or complete a real Stripe test checkout.

Useful Stripe CLI commands:

```bash
stripe trigger checkout.session.completed
stripe trigger customer.subscription.created
stripe trigger customer.subscription.updated
stripe trigger invoice.paid
```

## Run both locally with Docker

The project includes Docker Compose in [docker/docker-compose.yml](/Users/mozilla/Documents/projects/energy-atlas-ai/docker/docker-compose.yml) and wrapper targets in [Makefile](/Users/mozilla/Documents/projects/energy-atlas-ai/Makefile).

Build everything:

```bash
make build
```

Run all services attached:

```bash
make up
```

Run all services detached:

```bash
make upd
```

Show logs:

```bash
make logs
```

Stop everything:

```bash
make down
```

## Run Django and Chainlit separately with Make

Start Django only:

```bash
make up-django
```

Start Chainlit only:

```bash
make up-chainlit
```

Tail Django logs:

```bash
make logs-django
```

Tail Chainlit logs:

```bash
make logs-chainlit
```

Open a shell in the Django container:

```bash
make shell-django
```

Open a shell in the Chainlit container:

```bash
make shell-chainlit
```

## Local development workflow

Typical local workflow without Docker:

1. `poetry install`
2. `poetry run python app/manage.py migrate`
3. `poetry run python app/manage.py runserver`
4. `poetry run chainlit run chainlit/app.py -w`
5. `stripe listen --forward-to localhost:8000/stripe/webhook/`

Typical local workflow with Docker:

1. `make build`
2. `make upd`
3. `make logs-django`
4. `make logs-chainlit`
5. `stripe listen --forward-to localhost:8000/stripe/webhook/`

## Deployment

This repo includes Terraform under [infra/terraform/](/Users/mozilla/Documents/projects/energy-atlas-ai/infra/terraform) and deploy scripts under [scripts/](/Users/mozilla/Documents/projects/energy-atlas-ai/scripts).

Recommended env file split:

- `.env`: local development only
- `.env.production`: production secrets and server settings
- `.example.env`: committed template

Suggested `.gitignore` behavior:

- `.env` and `.env.*` stay uncommitted
- `.example.env` stays committed

### First deploy to a Terraform server

1. Provision the server with Terraform:

```bash
make tf-init
make tf-apply
```

After `make tf-apply`, check the current droplet IP:

```bash
make tf-output
```

The `droplet_ipv4` output is the IP your DNS records must point to.

2. Create a production env file locally:

```bash
cp .example.env .env.production
```

3. Fill in the real production values in `.env.production`.

At minimum, make sure `.env.production` includes:

```env
APP_DOMAIN=askenergyatlas.com
APP_URL=https://askenergyatlas.com
DJANGO_ALLOWED_HOSTS=askenergyatlas.com
```

4. Bootstrap the server:

```bash
make bootstrap-prod
```

This resolves the server IP from Terraform output and runs the bootstrap script with:

```bash
DEPLOY_ENV_FILE=.env.production
```

The bootstrap script uploads the code, uploads `.env.production` as the remote `.env`, builds the production Docker images, and starts the SSL-enabled production Docker Compose stack defined in `docker/docker-compose.production.yml`.

### DNS and SSL checklist

Before automatic HTTPS can work, your public DNS must point to the current Terraform droplet IP.

1. Get the current droplet IP:

```bash
make tf-output
```

2. In your DNS provider, point:

- `askenergyatlas.com` → current `droplet_ipv4`

3. Verify DNS locally:

```bash
dig +short askenergyatlas.com
```

4. Confirm the resolved IP matches Terraform output before expecting Caddy to issue TLS certificates.

If DNS still points to an older droplet, Let's Encrypt validation will fail even if the new server is healthy.

### Updating the app after deployment

Use:

```bash
make update-prod
```

This is the preferred update path after the initial deploy.

It:

- uploads the current repo contents
- uploads `.env.production` as the remote `.env`
- syncs code into the existing `/opt/energy-atlas-ai` directory
- preserves persistent paths such as `.env`, `data/`, `docker_volumes/`, `secrets/`, and `config/`
- rebuilds and restarts the Docker Compose services

You can also run the scripts directly:

```bash
./scripts/bootstrap.sh <server-ip> --ssh-key ~/.ssh/id_rsa --env-file .env.production
./scripts/update.sh <server-ip> --ssh-key ~/.ssh/id_rsa --env-file .env.production
```

### Notes on bootstrap vs update

- `bootstrap.sh` is intended for first-time setup.
- `update.sh` is intended for normal code deploys.
- Use `update.sh` for routine releases because it is non-destructive and preserves persistent directories.
- `bootstrap.sh` recreates the remote app directory, but now restores the uploaded production env file afterward when `--env-file` is used.
- Production Caddy configuration lives in `docker/Caddyfile.production` and uses automatic HTTPS for `APP_DOMAIN`.

## Notes

- Django runs on port `8000` in local development.
- Chainlit runs on port `8001` in Docker Compose.
- The preferred Stripe webhook forward target for local subscription activation is `/stripe/webhook/`, which updates [app/billing/models.py](/Users/mozilla/Documents/projects/energy-atlas-ai/app/billing/models.py).
- For compatibility, `/billing/webhook/` also points to the same subscription-sync handler.
