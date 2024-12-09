import pandas as pd
import matplotlib.pyplot as plt
import time

# Путь к файлу логов
log_file = './logs/metric_log.csv'
output_image = './logs/error_distribution.png'

while True:
    try:
        # Чтение файла
        if os.path.exists(log_file):
            data = pd.read_csv(log_file)
            if not data.empty:
                # Построение гистограммы
                plt.figure(figsize=(10, 6))
                plt.hist(data['absolute_error'], bins=20, color='skyblue', edgecolor='black')
                plt.title('Распределение абсолютных ошибок')
                plt.xlabel('Абсолютная ошибка')
                plt.ylabel('Частота')
                plt.savefig(output_image)
                plt.close()
                print('Гистограмма обновлена.')
        else:
            print('Файл metric_log.csv не найден.')

        time.sleep(10)

    except Exception as e:
        print(f'Ошибка: {e}')
