SHELL := /bin/bash

COMPOSE_FILE := docker/docker-compose.yml
DOCKER_COMPOSE := docker compose -f $(COMPOSE_FILE)
SERVICES := postgres django chainlit
ENV_FILE ?= .env
TF_DIR ?= infra/terraform
TF ?= terraform
TF_PLAN_FILE ?= tfplan
TF_IP_OUTPUT ?= droplet_ipv4

define tf_with_env
	set -a; . "$(ENV_FILE)"; set +a; cd "$(TF_DIR)" && $(TF) $(1)
endef

define tf_ip_shell
	set -a; . "$(ENV_FILE)"; set +a; cd "$(TF_DIR)" && $(TF) output -raw $(TF_IP_OUTPUT)
endef

.PHONY: help build up upd up-detached down clean start stop restart ps logs pull \
	tf-init tf-fmt tf-validate tf-plan tf-apply tf-destroy tf-output bootstrap \
	up-postgres up-django up-chainlit \
	logs-postgres logs-django logs-chainlit \
	restart-postgres restart-django restart-chainlit \
	shell-postgres shell-django shell-chainlit

help:
	@printf "Available targets:\n"
	@printf "  make build              Build all services\n"
	@printf "  make up                 Start all services attached\n"
	@printf "  make upd                Start all services in detached mode\n"
	@printf "  make up-detached        Alias for make upd\n"
	@printf "  make down               Stop and remove services\n"
	@printf "  make clean              Remove compose containers, networks, and local images\n"
	@printf "  make start              Start existing stopped services\n"
	@printf "  make stop               Stop running services\n"
	@printf "  make restart            Restart all services\n"
	@printf "  make ps                 Show service status\n"
	@printf "  make logs               Tail logs for all services\n"
	@printf "  make pull               Pull image updates where applicable\n"
	@printf "  make tf-init            Run terraform init in $(TF_DIR)\n"
	@printf "  make tf-fmt             Run terraform fmt in $(TF_DIR)\n"
	@printf "  make tf-validate        Run terraform validate in $(TF_DIR)\n"
	@printf "  make tf-plan            Run terraform plan and write $(TF_PLAN_FILE)\n"
	@printf "  make tf-apply           Apply the saved terraform plan\n"
	@printf "  make tf-destroy         Run terraform destroy in $(TF_DIR)\n"
	@printf "  make tf-output          Show terraform outputs\n"
	@printf "  make bootstrap          Bootstrap the server from terraform output $(TF_IP_OUTPUT)\n"
	@printf "  make up-<service>       Start one service in detached mode\n"
	@printf "  make logs-<service>     Tail logs for one service\n"
	@printf "  make restart-<service>  Restart one service\n"
	@printf "  make shell-<service>    Open a shell in one service container\n"
	@printf "\nServices: $(SERVICES)\n"

build:
	$(DOCKER_COMPOSE) build

up:
	$(DOCKER_COMPOSE) up

upd:
	$(DOCKER_COMPOSE) up -d

up-detached: upd

down:
	$(DOCKER_COMPOSE) down

clean:
	$(DOCKER_COMPOSE) down --remove-orphans --rmi local

start:
	$(DOCKER_COMPOSE) start

stop:
	$(DOCKER_COMPOSE) stop

restart:
	$(DOCKER_COMPOSE) restart

ps:
	$(DOCKER_COMPOSE) ps

logs:
	$(DOCKER_COMPOSE) logs -f

pull:
	$(DOCKER_COMPOSE) pull

tf-init:
	@$(call tf_with_env,init)

tf-fmt:
	@$(call tf_with_env,fmt)

tf-validate:
	@$(call tf_with_env,validate)

tf-plan:
	@$(call tf_with_env,plan -out=$(TF_PLAN_FILE))

tf-apply: tf-plan
	@$(call tf_with_env,apply $(TF_PLAN_FILE))

tf-destroy:
	@$(call tf_with_env,destroy)

tf-output:
	@$(call tf_with_env,output)

bootstrap:
	@set -e; \
	IP="$$( $(call tf_ip_shell) || true )"; \
	if [ -z "$$IP" ]; then \
		echo "ERROR: terraform output '$(TF_IP_OUTPUT)' is empty (or missing)."; \
		echo "Available outputs:"; \
		(set -a; . "$(ENV_FILE)"; set +a; cd "$(TF_DIR)" && $(TF) output || true); \
		exit 1; \
	fi; \
	echo "==> Bootstrapping server at $$IP"; \
	./scripts/bootstrap.sh "$$IP"

up-postgres:
	$(DOCKER_COMPOSE) up -d postgres

up-django:
	$(DOCKER_COMPOSE) up -d django

up-chainlit:
	$(DOCKER_COMPOSE) up -d chainlit

logs-postgres:
	$(DOCKER_COMPOSE) logs -f postgres

logs-django:
	$(DOCKER_COMPOSE) logs -f django

logs-chainlit:
	$(DOCKER_COMPOSE) logs -f chainlit

restart-postgres:
	$(DOCKER_COMPOSE) restart postgres

restart-django:
	$(DOCKER_COMPOSE) restart django

restart-chainlit:
	$(DOCKER_COMPOSE) restart chainlit

shell-postgres:
	$(DOCKER_COMPOSE) exec postgres sh

shell-django:
	$(DOCKER_COMPOSE) exec django sh

shell-chainlit:
	$(DOCKER_COMPOSE) exec chainlit sh
