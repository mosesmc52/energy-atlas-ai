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
stripe listen --forward-to localhost:8000/billing/webhook/
```

This repo also accepts:

```bash
stripe listen --forward-to localhost:8000/stripe/webhook/
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
5. `stripe listen --forward-to localhost:8000/billing/webhook/`

Typical local workflow with Docker:

1. `make build`
2. `make upd`
3. `make logs-django`
4. `make logs-chainlit`
5. `stripe listen --forward-to localhost:8000/billing/webhook/`

## Notes

- Django runs on port `8000` in local development.
- Chainlit runs on port `8001` in Docker Compose.
- The preferred Stripe webhook forward target for local subscription activation is `/billing/webhook/`, which updates [app/billing/models.py](/Users/mozilla/Documents/projects/energy-atlas-ai/app/billing/models.py).
- For compatibility, `/stripe/webhook/` also points to the same subscription-sync handler before the remaining `dj-stripe` URLs are mounted.
