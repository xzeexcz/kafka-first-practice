# Kafka: producer + 2 consumers

## Что реализовано
- `OrderMessageProducer` отправляет JSON-сообщение в Kafka асинхронно.
- `SingleMessageConsumer` читает сообщения по одному и использует auto-commit.
- `BatchMessageConsumer` накапливает минимум 10 сообщений, обрабатывает их циклом и делает один `commitSync` на пачку.
- У консьюмеров разные `group_id`, поэтому оба типа читают одни и те же сообщения независимо.
- При ошибках сериализации/десериализации пишется лог, приложение продолжает работу.

## Классы
- `AdminController` (`POST /admin/publish`) принимает команду на отправку.
- `OrderMessage` модель сообщения.
- `OrderMessageProducer` сериализует `OrderMessage` в JSON и публикует в Kafka.
- `SingleMessageConsumer` консьюмер с авто-коммитом.
- `BatchMessageConsumer` консьюмер с ручным коммитом после пачки.
- `ConsumerRunner` запускает оба консьюмера параллельно.

## Настройки
В `application.yaml`:
- `spring.kafka.producer.acks=all` и `retries=10` для гарантии At Least Once.
- `app.kafka.fetch-min-bytes` и `app.kafka.fetch-max-wait-ms` для пакетного чтения.
- `app.kafka.batch-size=10` для коммита после обработки 10 сообщений.

## Запуск
1. Поднять инфраструктуру и приложение:
   - `docker compose up --build -d`
2. Создать топик (3 партиции, RF=2):
   - `docker compose exec kafka1 kafka-topics --create --topic orders-topic --partitions 3 --replication-factor 2 --bootstrap-server kafka1:9092`
3. Проверить описание топика:
   - `docker compose exec kafka1 kafka-topics --describe --topic orders-topic --bootstrap-server kafka1:9092`

## Проверка работы
1. Отправить сообщение:
   - `curl -X POST http://localhost:8090/admin/publish -H "Content-Type: application/json" -d '{"orderId":"order-1","customerId":"customer-1","amount":123.45}'`
2. Проверить логи приложения:
   - `docker compose logs -f producer-app`
3. Убедиться, что в логах есть:
   - строка отправки от продюсера,
   - получение в `SingleMessageConsumer`,
   - получение в `BatchMessageConsumer` и коммит после обработки пачки.

## Примечание по replicas
`deploy.replicas: 2` указан в `docker-compose.yaml` по ТЗ. Для обычного Docker Compose масштабирование также можно выполнить командой:
- `docker compose up --build --scale producer-app=2 -d`
