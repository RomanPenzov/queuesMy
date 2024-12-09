import pika
import numpy as np
import json
from sklearn.datasets import load_diabetes
from datetime import datetime
import time

# Загружаем датасет о диабете
X, y = load_diabetes(return_X_y=True)

while True:
    try:
        # Случайный индекс строки
        random_row = np.random.randint(0, X.shape[0] - 1)
        # Уникальный идентификатор
        message_id = datetime.timestamp(datetime.now())
        
        # Создаем сообщения с идентификаторами
        message_y_true = {
            'id': message_id,
            'body': float(y[random_row])
        }
        message_features = {
            'id': message_id,
            'body': list(X[random_row])
        }

        # Подключение к RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()

        # Объявляем очереди
        channel.queue_declare(queue='y_true')
        channel.queue_declare(queue='features')

        # Отправляем сообщения
        channel.basic_publish(exchange='', routing_key='y_true', body=json.dumps(message_y_true))
        print(f'Отправлено сообщение в y_true: {message_y_true}')

        channel.basic_publish(exchange='', routing_key='features', body=json.dumps(message_features))
        print(f'Отправлено сообщение в features: {message_features}')

        # Закрываем соединение
        connection.close()

        # Задержка 10 секунд
        time.sleep(10)

    except Exception as e:
        print(f'Ошибка: {e}')
