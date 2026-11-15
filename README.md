# external-agents

Скилл для Codex (и любой другой агентной среды с терминалом): **совет внешних моделей с судьёй**. Одна и та же задача параллельно уходит нескольким моделям через OpenAI-совместимый API (по умолчанию — шлюз [JoinGonka](https://gate.joingonka.ai)), затем отдельная модель-судья оценивает ответы и собирает итоговый. Только стандартная библиотека Python 3, без зависимостей.

Главное в этой версии — **устойчивость к недоступным моделям**:

| Механизм | Что делает |
|---|---|
| Предварительная проверка (preflight) | Параллельный запрос на 1 токен (до 15 с) отсеивает мёртвые модели до начала совета, вместо ожидания по 180 с. |
| Подмена модели | Если модель упала окончательно (неисправимая ошибка или исчерпаны реплики), её место занимает следующая из запасного списка — совет добирает `--min-candidates` хороших ответов (по умолчанию 3). |
| Кворум | Судья стартует, как только пришло `--quorum` хороших ответов; отстающие отменяются. |
| Цепочка судей | `--judge-models A B C`: если судья недоступен, судит следующий. По возможности судья — не одна из моделей-кандидатов. `--judges N` — несколько независимых судей параллельно. |
| Память о здоровье моделей | `~/.cache/external-agents/health.json` (права 0600): сбои, таймауты, задержки, скорость. После двух сбоев подряд модель уходит на паузу (60 с, удваивается, максимум 30 мин), быстрые модели идут первыми. |
| Адаптивный таймаут | Дедлайн запроса подстраивается под наблюдаемую скорость модели (60 с … 2× базового). |
| Hydra-реплики и TTFT-хеджирование | При временной ошибке запускаются свежие копии запроса (до 5 попыток, жёсткий потолок 6). Если первый токен не пришёл за адаптивное время — сразу стартуют ещё две копии, побеждает первая. |
| Ошибка ключа | HTTP 401/403 = ключ отклонён для всего провайдера: совет сразу останавливается с понятной ошибкой, без подмен. |
| Продолжение обрезанных ответов | При `finish_reason=length` ответ дописывается с полной историей (ограниченное число раз). |

Всё, что произошло, видно в `summary.json` и `council.json`: какие модели отсеяны, кто кого подменил и почему, кворум, цепочка судей, итоговый судья, тайминги.

## Установка

```sh
git clone https://github.com/zalupa-ronaldo/external-agents ~/.codex/skills/external-agents
```

Ключ API храните в отдельном файле с правами 600 или в переменной окружения — **никогда** в аргументах командной строки или в файлах задач:

```sh
mkdir -p ~/.config/external-agents && chmod 700 ~/.config/external-agents
# положите ключ в ~/.config/external-agents/gonka.key, затем:
chmod 600 ~/.config/external-agents/gonka.key

python3 scripts/external_agents.py configure \
  --base-url https://gate.joingonka.ai/v1 --api chat-completions \
  --model zai-org/GLM-5.3-Flash --judge-model zai-org/GLM-5.3-Flash \
  --key-file ~/.config/external-agents/gonka.key
# или: --key-env GONKA_API_KEY

python3 scripts/external_agents.py status
python3 scripts/external_agents.py models
```

Необязательные поля конфига: `judge_models` (цепочка судей) и `fallback_models` (запасной список), задаются через `configure --judge-models ... --fallback-models ...`.

## Примеры

Совет по одной задаче (`task.json`):

```json
{"id": "review", "prompt": "Найди ошибки в парсере и предложи diff.", "context_files": {"parser.py": "/abs/path/parser.py"}}
```

```sh
python3 scripts/external_agents.py council --task task.json --output-dir runs/r1
# свои модели, запасные и цепочка судей:
python3 scripts/external_agents.py council --task task.json --output-dir runs/r2 \
  --models zai-org/GLM-5.3-Flash MiniMaxAI/MiniMax-M2.7 \
  --fallback-models deepseek-ai/DeepSeek-V4-Flash-0731 \
  --judge-models deepseek-ai/DeepSeek-V4-Flash-0731 zai-org/GLM-5.3-Flash \
  --min-candidates 2 --quorum 2
```

Результаты: `first.json` — первый готовый ответ (предварительно), `judge.json` — оценка судьи, `council.json` — сводка совета, `summary.json` — все попытки.

Пакет независимых задач:

```sh
python3 scripts/external_agents.py run --tasks tasks.json --output-dir runs/b1
```

Здоровье моделей:

```sh
python3 scripts/external_agents.py health          # посмотреть
python3 scripts/external_agents.py health --reset  # забыть
```

Полезные флаги: `--no-preflight`, `--ignore-health`, `--no-health`, `--ttft-hedge 0`, `--max-replicas N`, `--timeout S`, `--no-tools`.

## Безопасность

- **Никогда не отправляйте внешним моделям секреты**: ключи, токены, пароли, `.env`, приватные ключи, личные данные. Клиент блокирует очевидные файлы (`.env`, `*.pem`, `*.key`, файл ключа), но содержимое остальных файлов проверяете вы.
- Ключ передаётся дочернему процессу через stdin, не через argv, и вычищается из всех отчётов (`[REDACTED]`).
- Все артефакты (отчёты, конфиг, кэш здоровья) создаются с правами 0600.
- Только HTTPS (кроме localhost для тестов), редиректы запрещены, проверка TLS не отключается.
- Внешние модели не имеют доступа к shell и записи: они получают только явно приложенные снимки файлов и возвращают анализ и предлагаемые патчи. Ответы и вердикты судьи — недоверенные данные, проверяйте их сами.
- Отменённые локально запросы провайдер может продолжать обрабатывать (и тарифицировать).

## Тесты

```sh
cd scripts && python3 -m unittest test_external_agents
```

Офлайн-тесты с локальным мок-сервером, без реальных запросов и ключей.

## Лицензия

MIT, см. [LICENSE](LICENSE).
