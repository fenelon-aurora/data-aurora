# data-aurora

DAGs e pacote Python do Airflow da Aurora (https://pipeline.aurorapeople.ai).
A infraestrutura (ECS, bucket, sidecar de sync) está em `iac-aurora/pipeline`.

## Arquitetura

Mesmas regras do `ia-aurora` (hexagonal): a DAG é só o adaptador de entrada; a regra fica no pacote
`data_aurora`, que não importa Airflow e é testado sem ele.

`data_aurora` é só o espaço de nomes: todos os repositórios publicados no Airflow dividem o mesmo
processo Python, e um nome único evita que dois `services` ou `people` se confundam. O significado
fica nos subpacotes, um por domínio (o serviço com que a DAG trabalha), mais `shared`:

```
dags/                                  → adaptador de entrada: agenda, parâmetros, retries; chama o caso de uso
src/data_aurora/
  container.py                         → DI: único lugar que conhece as classes concretas
  shared/                              → comum a todos os domínios
    domain/   errors, entities (ServiceHealth), ports (SecretsPort, HealthPort)
    services/ healthcheck
    adapters/ secrets_manager (boto3)
    settings.py, logging.py
  people/                              → tudo que fala com o People
    domain/   entities, ports (PeoplePort)
    services/ close_expired_collections
    adapters/ people_http (requests)
tests/                                 → pytest; tests/people espelha o domínio; test_dags.py só roda com Airflow (CI)
```

Novo domínio (ex.: `ia`): `data_aurora/ia/{domain,services,adapters}`, ligação no `container.py`,
DAG em `dags/` e testes em `tests/ia/`.

Regras (as do `ia-aurora/.cursor/rules/02-ia-hexagonal-architecture.mdc`):

- caso de uso depende de port, nunca de `requests`/`boto3`;
- um domínio não importa outro; o que for comum vai para `shared`;
- DI só em `container.py`; configuração só em `shared/settings.py`;
- a task importa o container **dentro** da função, para o parse do scheduler não abrir conexão;
- log de entrada/saída/erro em todo caso de uso e adaptador; nunca logar segredo, token ou texto de colaborador;
- toda operação disparada por DAG precisa ser idempotente no serviço de destino.

## DAGs

| DAG | Agenda | Situação |
|---|---|---|
| `data_aurora_healthcheck` | manual | Confere pacote, configuração e `GET /health` do People |
| `people_close_expired_collections` | a cada 15 min | **Nasce pausada.** Depende da rota `POST /internal/cycles/close-expired-collections` do People (fase 3 de `people-aurora/docs/plan-prazo-encerramento-ciclo.md`), ainda não criada |

Toda DAG: `catchup=False`, `max_active_runs=1`, tag `data-aurora`.

## Configuração

Cada chave é lida da variável de ambiente (task definition, via iac-aurora) e, se ausente, da Airflow
Variable `data_aurora_<chave em minúsculas>` (Admin → Variables). Variable nunca guarda segredo, só o nome do segredo.

| Chave | Exemplo | Uso |
|---|---|---|
| `DATA_ENVIRONMENT` | `prod` | `dev`, `demo` ou `prod` (padrão `prod`) |
| `PEOPLE_API_URL` | `https://q6f4gw4v40.execute-api.us-east-1.amazonaws.com/api/v1` | base da API do People |
| `PEOPLE_SERVICE_KEY_SECRET_ID` | `data/prod/people-service-key` | segredo JSON `{"key": "..."}` enviado em `x-aurora-service-key` |
| `HTTP_TIMEOUT_SECONDS` | `30` | timeout das chamadas HTTP |
| `AWS_REGION` | `us-east-1` | região do Secrets Manager |

## Deploy

`.github/workflows/deploy-dags.yml`:

- **pull request:** lint e testes em Python 3.11 com `apache-airflow==2.8.1` (mesma versão da imagem), incluindo carregar as DAGs pelo `DagBag`;
- **push na `main`:** os mesmos testes e, passando, `aws s3 sync` de `dags/` e `src/` para
  `s3://aurora-airflow-dags-988477085226/data-aurora/` (com `--delete`), mais `.airflowignore` e `DEPLOYED_SHA`.
  O sidecar do Airflow copia o bucket para `/opt/airflow/dags/` a cada 30 s.

Credenciais: usa a role OIDC se a variável do repositório `AIRFLOW_DEPLOY_ROLE_ARN` estiver definida;
senão, os secrets `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (as chaves de CI dos outros serviços).

O deploy só copia código. Pacote novo em `requirements.txt` precisa entrar antes na imagem
(`iac-aurora/pipeline/docker/requirements.txt`) e ser publicado.

## Desenvolvimento

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/ruff check . && .venv/bin/pytest -q
```

`tests/test_dags.py` é pulado sem Airflow. Para rodar localmente, use Python 3.11 e instale
`apache-airflow==2.8.1` com o arquivo de constraints oficial (como no workflow).

## Pendências de infraestrutura (iac-aurora)

1. **Credencial do deploy:** cadastrar `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` neste repositório, ou
   (preferível) incluir `fenelon-aurora/data-aurora` em `github_allowed_repositories`, aplicar o
   Terraform de `pipeline/` e definir `AIRFLOW_DEPLOY_ROLE_ARN`. Hoje a role
   `aurora-airflow-github-actions-role` não existe.
2. **Chave de serviço do People:** criar o segredo `data/prod/people-service-key` e dar
   `secretsmanager:GetSecretValue` nesse ARN à role da task do worker do Airflow; o People passa a
   aceitar a mesma chave nas rotas `/internal/*` que a DAG chama.
3. **Configuração:** `PEOPLE_API_URL` e `PEOPLE_SERVICE_KEY_SECRET_ID` no worker (env ou Airflow Variable).
