# Bot Quotex — Documentação

Bot de opções binárias para a Quotex baseado em cruzamento de EMA 9/21, com filtros anti-lateralização, gestão de risco diária e seleção dinâmica de ativos.

> **Use somente conta DEMO.** O bot força a conta DEMO a cada conexão (`_set_demo`). Ele não foi validado para capital real.

Arquivo principal: `bot_quotex.py`.

---

## Sumário

1. [Requisitos e execução](#1-requisitos-e-execução)
2. [Arquitetura](#2-arquitetura)
3. [Configuração (`BotConfig`)](#3-configuração-botconfig)
4. [Estratégia de entrada](#4-estratégia-de-entrada)
5. [Seleção de ativos](#5-seleção-de-ativos)
6. [Gestão de risco](#6-gestão-de-risco)
7. [Ciclo de execução](#7-ciclo-de-execução)
8. [Conexão e recuperação de falhas](#8-conexão-e-recuperação-de-falhas)
9. [Persistência (SQLite)](#9-persistência-sqlite)
10. [Logs](#10-logs)
11. [Diagnóstico de problemas](#11-diagnóstico-de-problemas)
12. [Limitações conhecidas](#12-limitações-conhecidas)

---

## 1. Requisitos e execução

### Requisitos

- Python 3.13 (ambiente virtual em `pyquotex/.venv`).
- Biblioteca `pyquotex` deste repositório e as dependências de `requirements.txt`.
- Conta Quotex com acesso à conta DEMO.

### Variáveis de ambiente obrigatórias

| Variável          | Descrição          |
|-------------------|--------------------|
| `QUOTEX_EMAIL`    | E-mail da conta    |
| `QUOTEX_PASSWORD` | Senha da conta     |

Sem elas o bot encerra com: `Defina QUOTEX_EMAIL e QUOTEX_PASSWORD nas variáveis de ambiente.`

### Executar (PowerShell)

```powershell
cd "C:\Users\delma\Music\NEW PROJETO\pyquotex"
$env:QUOTEX_EMAIL = "seu@email"
$env:QUOTEX_PASSWORD = "sua_senha"
.\.venv\Scripts\python.exe bot_quotex.py
```

Parar: `Ctrl+C`.

No Windows o bot usa `asyncio.SelectorEventLoop`, porque o `curl_cffi` precisa de `add_reader`, que o loop Proactor padrão não oferece.

### Arquivos gerados

| Caminho              | Conteúdo                                                        |
|----------------------|-----------------------------------------------------------------|
| `session.json`       | Sessão (SSID) da Quotex. **Sensível: não compartilhe.**         |
| `logs/execucao.log`  | Log de execução (rotativo: 5 MB × 3 backups, UTF-8).            |
| `logs/operacoes.db`  | Banco SQLite com operações e saldo base diário.                 |

Todos os caminhos são relativos à pasta do script, não ao diretório de onde ele é executado.

---

## 2. Arquitetura

```
main()  ── supervisor: recria o bot se ele cair
  └─ QuotexSmartBot.start()  ── conecta, força DEMO, abre sessão do dia
       └─ run()  ── laço principal (a cada 0,5 s)
            ├─ _ensure_connected()   reconexão
            ├─ _roll_day()           troca de dia / libera stop diário
            ├─ _refresh_assets()     seleção de ativos (a cada 30 min)
            └─ _on_new_candle()      uma vez por vela
                 ├─ check_risk()     stop diário + stake
                 ├─ _scan()          busca velas e avalia sinal por ativo
                 │    ├─ _fetch_candles() / _load_history()
                 │    └─ evaluate_signal()
                 ├─ client.buy()     envia a ordem
                 └─ _await_result()  mede o resultado pelo saldo
```

Componentes:

| Componente          | Responsabilidade                                                      |
|---------------------|-----------------------------------------------------------------------|
| `BotConfig`         | Parâmetros (dataclass imutável).                                      |
| `TradeMemory`       | Persistência SQLite, cooldown, estatísticas por ativo.                |
| `rank_assets`       | Função pura de ranqueamento de ativos.                                |
| `ema_series`        | Série completa da EMA.                                                |
| `calculate_rsi`     | RSI de Wilder do último fechamento.                                   |
| `evaluate_signal`   | Regras de entrada; retorna `(sinal, motivo)`.                         |
| `crossover_signal`  | Atalho que retorna só o sinal.                                        |
| `QuotexSmartBot`    | Conexão, ciclo, varredura, ordens e resultados.                       |

---

## 3. Configuração (`BotConfig`)

Para alterar um parâmetro, edite o valor padrão em `BotConfig` no início de `bot_quotex.py`.

### Estratégia

| Parâmetro             | Padrão         | Descrição                                                                 |
|-----------------------|----------------|---------------------------------------------------------------------------|
| `timeframe`           | `300`          | Duração da vela e da expiração, em segundos (300 = M5, 60 = M1).          |
| `fast_ema`            | `9`            | Período da EMA rápida.                                                    |
| `slow_ema`            | `21`           | Período da EMA lenta.                                                     |
| `rsi_period`          | `14`           | Período do RSI.                                                           |
| `min_candles_history` | `45`           | Velas fechadas mínimas para estabilizar EMA21 e RSI14.                    |
| `min_body_ratio`      | `0.50`         | Corpo da vela de sinal ≥ 50% da média dos corpos das 5 velas anteriores.  |
| `rsi_call_range`      | `(50.0, 75.0)` | Faixa de RSI aceita para CALL.                                            |
| `rsi_put_range`       | `(25.0, 50.0)` | Faixa de RSI aceita para PUT.                                             |
| `cross_lookback`      | `2`            | Por quantas velas fechadas um cruzamento continua válido (1 = só a última). |
| `max_wick_ratio`      | `0.40`         | Pavio contra a direção da vela de sinal ≤ 40% do range (máxima − mínima). |
| `atr_period`          | `14`           | Período do ATR (Wilder).                                                  |
| `atr_ratio_range`     | `(0.60, 2.00)` | Faixa aceita de ATR atual ÷ média do true range da série.                 |

### Gestão de risco

| Parâmetro                | Padrão | Descrição                                                        |
|--------------------------|--------|------------------------------------------------------------------|
| `risk_per_trade_pct`     | `0.02` | Stake = 2% do saldo atual.                                       |
| `max_daily_loss_pct`     | `0.03` | Stop loss diário: −3% do saldo base do dia.                      |
| `daily_target_pct`       | `0.03` | Stop win diário: +3% do saldo base do dia.                       |
| `max_consecutive_losses` | `2`    | Losses seguidos no mesmo ativo que ativam o cooldown.            |
| `cooldown_seconds`       | `900`  | Pausa do ativo após a sequência de losses (15 min).              |
| `min_amount`             | `5.0`  | Stake mínima aceita pela conta.                                  |

### Temporização e rede

| Parâmetro               | Padrão | Descrição                                                              |
|-------------------------|--------|------------------------------------------------------------------------|
| `max_entry_delay`       | `20`   | Segundos após a abertura da vela em que ainda é permitido entrar.      |
| `candle_fetch_attempts` | `3`    | Tentativas de obter velas por ativo.                                   |
| `candle_fetch_interval` | `0.5`  | Segundos entre tentativas.                                             |
| `expiry_extra`          | `3`    | Folga após a expiração antes de ler o saldo.                           |
| `balance_stable_reads`  | `2`    | Leituras iguais seguidas para considerar o saldo estável.              |
| `balance_max_reads`     | `10`   | Máximo de leituras de saldo.                                           |
| `reconnect_wait`        | `45`   | Segundos aguardando a reconexão automática antes de recriar a conexão. |
| `result_extra_wait`     | `60`   | Segundos além da expiração aguardando o resultado da ordem.            |
| `restart_min_wait`      | `30`   | Espera inicial do supervisor antes de recriar o bot.                   |
| `restart_max_wait`      | `300`  | Espera máxima do supervisor (backoff dobrando até este valor).         |
| `login_block_min_wait`  | `900`  | Espera após login bloqueado (Cloudflare 403 ou `Login failed`).        |
| `login_block_max_wait`  | `3600` | Espera máxima após bloqueios seguidos de login.                        |

### Seleção de ativos

| Parâmetro          | Padrão | Descrição                                                         |
|--------------------|--------|-------------------------------------------------------------------|
| `min_payout`       | `0.80` | Payout mínimo para o ativo ser candidato.                         |
| `pool_size`        | `10`   | Candidatos avaliados (os de maior payout).                        |
| `top_n`            | `5`    | Ativos na lista de operação.                                      |
| `min_sample`       | `15`   | Operações mínimas por ativo antes de ser ranqueado por desempenho.|
| `rank_window`      | `30`   | Ranking usa só as últimas N operações WIN/LOSS do ativo.          |
| `reselect_seconds` | `1800` | Intervalo de reavaliação da lista (30 min).                       |
| `scan_concurrency` | `3`    | Ativos buscados em paralelo por vela.                             |

---

## 4. Estratégia de entrada

A análise usa apenas **velas fechadas**. A entrada é feita na abertura da vela seguinte, com expiração igual a `timeframe`.

`evaluate_signal` aplica os filtros abaixo, nesta ordem. O primeiro que reprovar define o motivo registrado no log.

| # | Filtro | CALL | PUT | Motivo no log |
|---|--------|------|-----|---------------|
| 0 | Histórico suficiente | ≥ `min_candles_history` velas | idem | `histórico curto` |
| 1 | Cruzamento recente | EMA9 cruzou para cima da EMA21 nas últimas `cross_lookback` velas e continua acima | EMA9 cruzou para baixo e continua abaixo | `sem cruzamento` |
| 2 | Força do cruzamento | Distância EMA9 − EMA21 aumentou na última vela | Distância aumentou para baixo | `EMAs convergindo` |
| 3 | Tendência | EMA21 subiu na última vela | EMA21 caiu na última vela | `EMA21 contra` |
| 4 | Vela de sinal | Vela de alta e fechamento acima da EMA9 | Vela de baixa e fechamento abaixo da EMA9 | `vela contra` |
| 5 | Corpo relevante | Corpo ≥ `min_body_ratio` × média das 5 velas anteriores | idem | `corpo pequeno` |
| 6 | Pavio contra | Pavio superior ≤ `max_wick_ratio` × range | Pavio inferior ≤ `max_wick_ratio` × range | `pavio contra <n>%` |
| 7 | Volatilidade | ATR ÷ média do true range dentro de `atr_ratio_range` | idem | `ATR baixo <n>` / `ATR alto <n>` |
| 8 | RSI | Dentro de `rsi_call_range` | Dentro de `rsi_put_range` | `RSI <valor>` |

Quando todos passam, o retorno é um `Signal` (NamedTuple) com `direction`, `ema_fast`, `ema_slow`, `rsi`, `cross_ts`, `wick_ratio` e `atr_ratio`.

Se a API não entregar máxima e mínima de uma vela, o bot usa o corpo (`max/min(open, close)`). Nesse caso o pavio vale 0 e o ATR é medido pelos corpos.

### Um cruzamento, uma entrada

Com `cross_lookback = 2`, o mesmo cruzamento continua visível na vela seguinte. O bot guarda em `traded_cross` o timestamp do último cruzamento operado por ativo e não entra duas vezes no mesmo cruzamento. No log aparece `cruzamento já operado`.

Esse controle fica em memória. Se o bot reiniciar entre as duas velas, o mesmo cruzamento pode gerar uma segunda entrada.

### Indicadores

- **EMA:** semente pela média simples dos primeiros `period` fechamentos; depois `ema = preço·k + ema_anterior·(1−k)`, com `k = 2/(period+1)`.
- **RSI de Wilder:** médias de ganho e perda suavizadas com `(média·(n−1) + valor)/n`. Retorna 100 quando não há perdas.
- **ATR de Wilder:** true range = `max(máx − mín, |máx − fech_ant|, |mín − fech_ant|)`, suavizado como o RSI. O regime é o ATR dividido pela média de todos os true ranges da série (≈ 1 = volatilidade normal do ativo).
- **Pavio contra:** CALL usa `máx − max(abertura, fech)`; PUT usa `min(abertura, fech) − mín`. Dividido pelo range da vela.

### Escolha entre ativos

Os ativos são avaliados na ordem da lista ativa (ranking). O **primeiro** com sinal válido é operado. No máximo uma entrada por vela.

---

## 5. Seleção de ativos

Executada no início e a cada `reselect_seconds` (`_refresh_assets`):

1. **Candidatos:** todos os ativos com payout ≥ `min_payout` no timeframe configurado.
2. **Pool:** os `pool_size` de maior payout.
3. **Ranking** (`rank_assets`):
   - Ativos com ≥ `min_sample` operações (WIN/LOSS, só as últimas `rank_window`): vantagem = taxa de acerto − breakeven. Entram só os com vantagem > 0, ordenados pela vantagem.
   - Se faltarem vagas para `top_n`, completa com os ativos **menos testados** (fase de coleta).
4. Ativos que saíram da lista têm o stream de velas cancelado e o cache apagado.

**Breakeven** = `1 / (1 + payout)`. Exemplo: payout 92% → breakeven 52,1%.

Ativo com amostra completa e resultado abaixo do breakeven é excluído da lista.

---

## 6. Gestão de risco

Verificada a cada vela em `check_risk`, antes de qualquer análise.

| Regra | Cálculo | Efeito |
|-------|---------|--------|
| Saldo base do dia | Saldo no primeiro início do dia, salvo em `daily_sessions` | Mantido mesmo se o bot reiniciar no mesmo dia. |
| Stop loss diário | PnL ≤ −`max_daily_loss_pct` × saldo base | Pausa até o dia seguinte. |
| Stop win diário | PnL ≥ `daily_target_pct` × saldo base | Pausa até o dia seguinte. |
| Stake | `risk_per_trade_pct` × saldo atual, arredondada em 2 casas | Se < `min_amount`, a entrada é ignorada. |
| Cooldown por ativo | Últimas `max_consecutive_losses` operações do ativo **no dia** são todas LOSS | Ativo pausado por `cooldown_seconds` a partir do resultado. Um DRAW interrompe a sequência. |
| Uma entrada por vela e ativo | `already_traded_candle` no banco | Evita duplicar ordem após reinício. |

O PnL é a diferença entre o saldo atual da conta e o saldo base. **Operações manuais na mesma conta entram nesse cálculo.**

---

## 7. Ciclo de execução

### Laço principal (`run`)

A cada 0,5 s:

1. Garante a conexão.
2. Troca o dia se a data mudou (novo saldo base; libera stop diário).
3. Se o stop diário estiver ativo, dorme 30 s e repete.
4. Reavalia ativos se passou `reselect_seconds`.
5. Se começou uma vela nova, chama `_on_new_candle`.

Erros no ciclo são registrados e o laço tenta de novo com backoff de 5 s, dobrando até 60 s.

### Nova vela (`_on_new_candle`)

1. Se passaram mais de `max_entry_delay` segundos desde a abertura, ignora a vela (`Vela iniciada há muito tempo`). Isso é normal logo após iniciar o bot.
2. Lê o saldo e aplica `check_risk`.
3. `_scan`: filtra ativos já operados nesta vela ou em cooldown; busca velas em paralelo (até `scan_concurrency`); avalia o sinal de cada um.
4. Sem sinal: registra uma linha por vela com o motivo de cada ativo.
5. Com sinal: confere o atraso de novo, envia `client.buy(stake, ativo, direção, timeframe)`.
6. Aguarda e registra o resultado.

### Obtenção de velas

- `_load_history` busca 10 velas quando já há cache suficiente, ou `min_candles_history + 15` na primeira carga. Se `get_candles` vier vazio na primeira carga, usa `get_historical_candles`.
- Timestamps são normalizados (segundos ou milissegundos) para o início exato da vela.
- O cache por ativo mantém as últimas `min_candles_history + 25` velas.
- `_fetch_candles` só libera a série se a última e a penúltima vela fechada existirem e as 3 últimas forem contíguas. Até `candle_fetch_attempts` tentativas.

### Resultado da ordem (`_await_result`)

1. Com ID da ordem: aguarda o evento de fechamento (`check_win`), com timeout de `timeframe + result_extra_wait`.
2. Sem ID ou sem evento: espera a expiração.
3. Lê o saldo até ficar estável (`balance_stable_reads` leituras iguais, máximo `balance_max_reads`).
4. Lucro = saldo depois − saldo antes. Positivo = WIN, negativo = LOSS, zero = DRAW.

---

## 8. Conexão e recuperação de falhas

Três camadas, da mais rápida para a mais lenta:

1. **Reconexão automática do websocket.** `_ensure_connected` espera até `reconnect_wait` segundos pela própria biblioteca reconectar. Ela não abre uma segunda conexão com o mesmo SSID, porque o servidor rejeita as duas.
2. **Recriação da conexão.** Sem sucesso, chama `client.connect()`. Se falhar, o erro volta para o laço, que tenta de novo com backoff (5–60 s).
3. **Supervisor (`main`).** Se `start()` falhar (por exemplo, `Timeout waiting for authorization`) ou o bot encerrar, recria o bot inteiro. Espera `restart_min_wait` segundos, dobrando até `restart_max_wait`. Se o bot rodou mais de 10 min antes de cair, a espera volta ao mínimo.

Após qualquer reconexão a conta é forçada para DEMO de novo.

### Login bloqueado (HTTP 403)

Quando o SSID salvo é rejeitado, a biblioteca faz login HTTP de novo. Logins repetidos em pouco tempo fazem o Cloudflare marcar o IP (`HTTP 403: blocked by Cloudflare challenge`). A Quotex também passa a responder `Login failed. E-mail ou senha inválidos`, mesmo com a senha certa.

Proteções:

- O supervisor identifica esses erros (`is_login_block`) e espera `login_block_min_wait` (15 min), dobrando até `login_block_max_wait` (60 min). Não fica tentando a cada 30 s.
- Só uma instância roda por vez (trava em `logs/bot.lock`). Uma segunda instância encerra com `Outra instância do bot já está rodando.`
- `session.json` fica sempre na pasta do script, de qualquer pasta que o bot seja iniciado. Isso evita um login novo por sessão não encontrada.
- Avisos da biblioteca `pyquotex` (login, websocket) também vão para `logs/execucao.log`.

Operações que estavam `EXECUTED` quando o bot caiu viram `UNRESOLVED` no próximo início.

---

## 9. Persistência (SQLite)

Arquivo: `logs/operacoes.db`.

### Tabela `daily_sessions`

| Coluna            | Tipo | Descrição                 |
|-------------------|------|---------------------------|
| `trade_date`      | TEXT | Data `YYYY-MM-DD` (chave) |
| `initial_balance` | REAL | Saldo base do dia         |

### Tabela `trades`

| Coluna         | Tipo    | Descrição                                                           |
|----------------|---------|---------------------------------------------------------------------|
| `id`           | INTEGER | Chave autoincremental                                               |
| `trade_date`   | TEXT    | Data da operação                                                    |
| `candle_ts`    | INTEGER | Timestamp de abertura da vela de entrada                            |
| `asset`        | TEXT    | Ativo (ex.: `EURUSD_otc`)                                           |
| `direction`    | TEXT    | `call` ou `put`                                                     |
| `amount`       | REAL    | Stake                                                               |
| `sma_fast`     | REAL    | Valor da EMA rápida (nome de coluna mantido por compatibilidade)    |
| `sma_slow`     | REAL    | Valor da EMA lenta (idem)                                           |
| `status`       | TEXT    | Ver tabela abaixo                                                   |
| `profit`       | REAL    | Lucro ou prejuízo medido pelo saldo                                 |
| `error_reason` | TEXT    | Motivo de falha, quando houver                                      |
| `created_at`   | INTEGER | Timestamp Unix do registro                                          |
| `rsi`          | REAL    | RSI da vela de sinal                                                |
| `wick_ratio`   | REAL    | Pavio contra a direção ÷ range da vela de sinal                     |
| `atr_ratio`    | REAL    | ATR ÷ média do true range no momento do sinal                       |

Bancos antigos recebem `rsi`, `wick_ratio` e `atr_ratio` por `ALTER TABLE` ao iniciar. Operações antigas ficam com `NULL`.

### Status das operações

| Status        | Significado                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `EXECUTED`    | Ordem aceita, aguardando resultado.                                         |
| `WIN`         | Saldo aumentou.                                                             |
| `LOSS`        | Saldo diminuiu.                                                             |
| `DRAW`        | Saldo não mudou.                                                            |
| `REJECTED`    | Ordem não saiu ou foi recusada. Não bloqueia nova entrada na mesma vela.    |
| `UNCONFIRMED` | Timeout na confirmação. A ordem **pode** ter sido executada.                |
| `UNRESOLVED`  | Sem resultado (queda do bot ou falha ao ler o resultado).                   |

Só `WIN` e `LOSS` contam para o ranking de ativos.

### Consultas úteis

```sql
-- Resultado por ativo
SELECT asset, SUM(status='WIN') AS wins, SUM(status='LOSS') AS losses,
       ROUND(1.0*SUM(status='WIN')/COUNT(*), 3) AS taxa, ROUND(SUM(profit), 2) AS pnl
FROM trades WHERE status IN ('WIN','LOSS') GROUP BY asset ORDER BY taxa DESC;

-- Taxa de acerto por faixa de pavio (avalia o filtro com dados reais)
SELECT ROUND(wick_ratio, 1) AS pavio, COUNT(*) AS ops,
       ROUND(1.0*SUM(status='WIN')/COUNT(*), 3) AS taxa
FROM trades WHERE status IN ('WIN','LOSS') AND wick_ratio IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- PnL por dia
SELECT trade_date, COUNT(*) AS ops, ROUND(SUM(profit), 2) AS pnl
FROM trades GROUP BY trade_date ORDER BY trade_date;
```

Executar pelo Python:

```powershell
.\.venv\Scripts\python.exe -c "import sqlite3;c=sqlite3.connect('logs/operacoes.db');print(c.execute('SELECT trade_date,COUNT(*),SUM(profit) FROM trades GROUP BY 1').fetchall())"
```

---

## 10. Logs

Formato: `AAAA-MM-DD HH:MM:SS [NÍVEL] mensagem`, no console e em `logs/execucao.log`.

### Mensagens principais

| Mensagem | Significado |
|----------|-------------|
| `Bot iniciado (M5 \| EMA 9/21) \| Saldo base ... \| Atual ...` | Conexão concluída. |
| `Ativo X (M5) \| payout ... \| breakeven ... \| amostra n/15 \| taxa ...` | Ativo na lista de operação. |
| `Vela HH:MM sem entrada \| ATIVO=motivo, ...` | Vela analisada, nenhum sinal. Uma linha por vela: é o **sinal de vida** do bot. |
| `ENTRADA CALL em X \| R$ ... \| EMA9=... EMA21=... \| RSI=... \| pavio=...% \| ATR=...` | Ordem enviada. |
| `RESULTADO X: GAIN/LOSS/EMPATE \| valor: R$ ...` | Resultado medido. |
| `Vela iniciada há muito tempo. Sinal desta vela ignorado.` | Normal logo após iniciar ou reconectar. |
| `X em cooldown por sequência de loss (Ns restantes).` | Ativo pausado. |
| `BLOQUEIO DIÁRIO: stop win/loss atingido` | Bot pausado até o dia seguinte. |
| `Conexão caiu. Aguardando reconexão automática...` | Websocket caiu. |
| `Bot caiu: ... Reiniciando em Ns.` | Supervisor vai recriar o bot. |
| `velas insuficientes (n/45) ou última vela HH:MM ausente` | API não entregou as velas a tempo. |
| `Ordem sem confirmação (pode ter sido executada): Timeout` | Confira na plataforma. |

### Leitura dos motivos

Uma vela com todos os ativos em `sem cruzamento` é o caso mais comum: em M5 um cruzamento por ativo é raro. Se um motivo diferente domina por horas (por exemplo, sempre `RSI` ou `corpo pequeno`), esse filtro é o candidato a ajuste.

---

## 11. Diagnóstico de problemas

| Sintoma | Causa provável | O que fazer |
|---------|----------------|-------------|
| Log para de crescer | Bot travado ou encerrado. | Verifique se o processo está ativo e as últimas linhas do log. |
| `Websocket connection rejected` | Sessão inválida ou conexão duplicada com o mesmo SSID. | Aguarde o supervisor. Se persistir, apague `session.json` e reinicie para refazer o login. |
| `Timeout waiting for authorization` | Login ou sessão não autorizados. | Mesmo procedimento acima; confira e-mail e senha. |
| Nenhuma entrada, mas linhas `Vela ... sem entrada` aparecem | Filtros sem sinal no período. | Analise os motivos predominantes antes de mudar parâmetros. |
| Muitas `velas insuficientes` | API lenta ou ativo sem histórico. | O ativo é pulado na vela; costuma se resolver na troca de lista. |
| Resultados errados no banco | Operação manual na mesma conta durante a ordem do bot. | Não opere manualmente na conta DEMO enquanto o bot roda. |
| `Login bloqueado: HTTP 403 ...` ou `Login failed` | IP marcado pelo Cloudflare após muitos logins. | Deixe o supervisor esperar. Não reinicie o bot várias vezes; cada início é um login novo. |
| `Outra instância do bot já está rodando.` | Já existe um bot aberto. | Use o que está rodando ou encerre-o com `Ctrl+C`. |
| Bot pausado o dia todo | Stop win ou stop loss diário atingido. | Comportamento esperado; libera à meia-noite. |

### Abrir a Quotex no navegador com o bot rodando

O navegador usa uma sessão própria, diferente de `session.json`. Se a Quotex invalidar a sessão do bot nesse momento, o supervisor tenta religar. Para confirmar, acompanhe o log: as linhas `Vela HH:MM ...` devem continuar a cada 5 minutos.

---

## 12. Limitações conhecidas

- **Resultado medido pelo saldo.** Qualquer movimentação na conta durante a ordem distorce WIN/LOSS e o PnL diário.
- **Estratégia não validada em dados reais.** Os ajustes dos filtros foram comparados só em dados simulados. A taxa de acerto real precisa ser medida com a amostra do banco (`min_sample` por ativo).
- **Ranking com janela curta.** Com `rank_window = 30`, a taxa de um ativo oscila mais; um ativo pode sair e voltar à lista.
- **Filtros de pavio e ATR sem validação.** Os limites padrão são pontos de partida. Ajuste só depois de medir com as colunas `wick_ratio` e `atr_ratio`.
- **`traded_cross` em memória.** Um reinício entre duas velas pode repetir a entrada no mesmo cruzamento.
- **Uma entrada por vela.** Sinais simultâneos em vários ativos: só o primeiro do ranking é operado.
- **Payout lido na seleção.** O payout pode mudar entre reavaliações (30 min).
