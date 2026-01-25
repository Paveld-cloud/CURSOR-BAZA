FROM python:3.12-slim

# Настройки Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Создаем непривилегированного пользователя для безопасности
RUN useradd -m appuser && chown -R appuser /app

# Сначала копируем зависимости для кэширования слоев Docker
COPY requirements.txt ./
RUN python -m pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Копируем весь проект и меняем владельца 
COPY --chown=appuser:appuser . . 

# Переключаемся на пользователя
USER appuser

# Используем -u для моментального вывода логов 
CMD ["python", "-u", "main.py"]
