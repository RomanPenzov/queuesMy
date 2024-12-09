import pika
import json
import pandas as pd
import os
from datetime import datetime


if not os.path.exists('./logs/metric_log.csv'):
    with open('./logs/metric_log.csv', 'w') as log_file:
        log_file.write('id,y_true,y_pred,absolute_error\n')

# Словари для хранения данных
y_true_data = {}
y_pred_data = {}

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очереди
    channel.queue_declare(queue='y_true')
    channel.queue_declare(queue='y_pred')

    # Обработка сообщений из y_true
    def callback_y_true(ch, method, properties, body):
        message = json.loads(body)
        y_true_data[message['id']] = message['body']
        process_messages()

    # Обработка сообщений из y_pred
    def callback_y_pred(ch, method, properties, body):
        message = json.loads(body)
        y_pred_data[message['id']] = message['body']
        process_messages()

    # Функция обработки данных
    def process_messages():
        common_ids = set(y_true_data.keys()) & set(y_pred_data.keys())
        for message_id in common_ids:
            y_true = y_true_data.pop(message_id)
            y_pred = y_pred_data.pop(message_id)
            absolute_error = abs(y_true - y_pred)

            # Запись в CSV
            with open(log_file, 'a') as f:
                f.write(f'{message_id},{y_true},{y_pred},{absolute_error}\n')
            print(f'Записано: id={message_id}, y_true={y_true}, y_pred={y_pred}, error={absolute_error}')

    # Подписываемся на очереди
    channel.basic_consume(queue='y_true', on_message_callback=callback_y_true, auto_ack=True)
    channel.basic_consume(queue='y_pred', on_message_callback=callback_y_pred, auto_ack=True)

    print('...Ожидание сообщений')
    channel.start_consuming()

except Exception as e:
    print(f'Ошибка: {e}')
