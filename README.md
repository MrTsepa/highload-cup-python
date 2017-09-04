# highload-cup-python
Решение [highload cup](https://highloadcup.ru/) на фреймворке bottle

Связка технологий: веб: `python + bottle + gevent`, бд: `MongoDB`, а также `nginx` для балансировки и кэширования.

## Результаты
На маленьких данных: `628` секунд.

На больших данных: `Out of memory`.
