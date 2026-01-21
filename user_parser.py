import asyncio
import logging
import time
import os
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetHistoryRequest
from database import db

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация (ЗАМЕНИТЕ НА СВОИ ДАННЫЕ!)
API_ID = 37780238 # Ваш api_id с my.telegram.org
API_HASH = 'fbfe8a419fea2f1ee79b9cc32bc49e18' # Ваш api_hash
PHONE_NUMBER = '+959760950133'  # Номер аккаунта для парсера

class ParserWorker:
    def __init__(self):
        self.client = None
        self.is_running = True
        self.session_file = 'parser_session.session'
    
    async def initialize_client(self):
        """Инициализация клиента Telegram с обработкой ошибок"""
        try:
            self.client = TelegramClient(self.session_file, API_ID, API_HASH)
            
            # Настройка автоматической обработки FloodWait
            self.client.flood_sleep_threshold = 60  # Секунд
            
            await self.client.connect()
            logger.info("Подключение к Telegram установлено")
            
            if not await self.client.is_user_authorized():
                logger.info("Сессия не авторизована. Запрашиваю код...")
                await self.client.send_code_request(PHONE_NUMBER)
                code = input("📱 Введите код из Telegram: ")
                
                try:
                    await self.client.sign_in(PHONE_NUMBER, code)
                except errors.SessionPasswordNeededError:
                    password = input("🔐 Требуется пароль двухфакторной аутентификации: ")
                    await self.client.sign_in(password=password)
            
            logger.info("✅ Клиент Telegram успешно авторизован")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации клиента: {e}")
            return False
    
    async def check_task_cancelled(self, task_id):
        """Проверяет, была ли задача отменена"""
        task_info = db.get_task_info(task_id)
        return task_info and task_info['status'] == 'cancelled'
    
    async def get_active_users_fast(self, chat, max_users=300, min_messages=2, task_id=None):
        """
        Оптимизированный метод получения активных пользователей
        с проверкой отмены задачи
        """
        active_users = {}
        total_messages_checked = 0
        
        try:
            # Получаем последние сообщения из чата (до 1000)
            logger.info(f"📊 Анализирую историю сообщений чата...")
            
            offset_id = 0
            batch_count = 0
            
            while total_messages_checked < 1000 and len(active_users) < max_users:
                # Проверяем, не была ли отменена задача
                if task_id and await self.check_task_cancelled(task_id):
                    logger.info(f"⏹️ Задача #{task_id} отменена, прекращаю парсинг")
                    return []
                
                try:
                    # Получаем пачку сообщений (100 за раз)
                    messages = await self.client.get_messages(
                        chat, 
                        limit=100,
                        offset_id=offset_id
                    )
                    
                    if not messages:
                        break
                    
                    batch_count += 1
                    total_messages_checked += len(messages)
                    
                    # Обрабатываем сообщения в этой пачке
                    for msg in messages:
                        if hasattr(msg, 'sender_id') and msg.sender_id:
                            sender_id = msg.sender_id
                            
                            # Получаем информацию об отправителе
                            try:
                                sender = await self.client.get_entity(sender_id)
                                
                                # Проверяем, есть ли username
                                if hasattr(sender, 'username') and sender.username:
                                    user_key = sender.username.lower()
                                    
                                    if user_key not in active_users:
                                        active_users[user_key] = {
                                            'id': sender.id,
                                            'username': sender.username,
                                            'first_name': getattr(sender, 'first_name', ''),
                                            'last_name': getattr(sender, 'last_name', ''),
                                            'messages_count': 1
                                        }
                                    else:
                                        active_users[user_key]['messages_count'] += 1
                                        
                                    # Если пользователь достиг порога активности, помечаем
                                    if active_users[user_key]['messages_count'] >= min_messages:
                                        active_users[user_key]['is_active'] = True
                            except Exception as e:
                                logger.debug(f"Не удалось получить отправителя {sender_id}: {e}")
                                continue
                    
                    # Обновляем offset_id для следующей пачки
                    offset_id = messages[-1].id
                    
                    logger.info(f"Обработано сообщений: {total_messages_checked}, "
                               f"Найдено уникальных пользователей: {len(active_users)}")
                    
                    # Пауза между пачками для избежания FloodWait
                    if batch_count % 5 == 0:
                        await asyncio.sleep(2)
                        
                except errors.FloodWaitError as e:
                    logger.warning(f"⏳ FloodWait! Ждем {e.seconds} секунд...")
                    await asyncio.sleep(e.seconds)
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при получении сообщений: {e}")
                    break
            
            # Фильтруем только активных пользователей (2+ сообщений)
            result = []
            for user_data in active_users.values():
                if user_data.get('is_active', False):
                    result.append(user_data)
                    
            logger.info(f"✅ Найдено активных пользователей (2+ сообщений): {len(result)}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Ошибка в get_active_users_fast: {e}")
            return []
    
    async def process_task(self, task):
        """Обработка одной задачи парсинга с проверкой отмены"""
        task_id = task['id']
        chat_link = task['chat_link']
        max_users = task['limit_count']
        
        logger.info(f"🔄 Начинаю обработку задачи #{task_id}: {chat_link}")
        
        try:
            # Проверяем, не была ли задача отменена перед началом
            if await self.check_task_cancelled(task_id):
                logger.info(f"⏹️ Задача #{task_id} была отменена до начала обработки")
                return {
                    'success': False,
                    'error': 'Задача отменена',
                    'cancelled': True
                }
            
            # Получаем сущность чата
            chat = await self.client.get_entity(chat_link)
            chat_title = chat.title if hasattr(chat, 'title') else chat.username
            logger.info(f"📁 Чат: {chat_title}")
            
            # Используем оптимизированный метод
            active_users = await self.get_active_users_fast(
                chat, max_users, min_messages=2, task_id=task_id
            )
            
            # Проверяем, не была ли задача отменена во время парсинга
            if await self.check_task_cancelled(task_id):
                logger.info(f"⏹️ Задача #{task_id} была отменена во время парсинга")
                return {
                    'success': False,
                    'error': 'Задача отменена',
                    'cancelled': True
                }
            
            # Сохраняем результаты в файл
            filename = await self.save_results(active_users, chat_title)
            
            if active_users:
                logger.info(f"✅ Задача #{task_id} завершена. Найдено активных: {len(active_users)}")
                return {
                    'success': True,
                    'filename': filename,
                    'users_found': len(active_users),
                    'chat_title': chat_title
                }
            else:
                logger.warning(f"⚠️ Задача #{task_id}: активные пользователи не найдены")
                return {
                    'success': True,
                    'filename': None,
                    'users_found': 0,
                    'chat_title': chat_title,
                    'note': 'Активные пользователи не найдены'
                }
                
        except errors.FloodWaitError as e:
            logger.error(f"⏳ FloodWaitError для задачи #{task_id}: {e.seconds} секунд")
            return {
                'success': False,
                'error': f'FloodWait: {e.seconds} секунд',
                'retry_after': e.seconds
            }
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке задачи #{task_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def save_results(self, users, chat_title):
        """Сохраняет результаты в файл"""
        if not users:
            return None
        
        # Создаем безопасное имя файла
        safe_title = "".join(c for c in chat_title if c.isalnum() or c in (' ', '-', '_')).rstrip()
        timestamp = int(time.time())
        filename = f"results/{safe_title}_{timestamp}.txt"
        
        # Создаем папку results, если её нет
        os.makedirs("results", exist_ok=True)
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"Активные пользователи из '{chat_title}'\n")
                f.write(f"Всего найдено: {len(users)}\n")
                f.write(f"Время парсинга: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                
                # Сортируем по количеству сообщений (по убыванию)
                users_sorted = sorted(users, key=lambda x: x['messages_count'], reverse=True)
                
                for i, user in enumerate(users_sorted, 1):
                    full_name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
                    f.write(f"{i:3}. @{user['username']:20} ")
                    f.write(f"- {full_name:20} ")
                    f.write(f"(сообщений: {user['messages_count']:3})\n")
            
            logger.info(f"💾 Результаты сохранены в {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения файла: {e}")
            return None
    
    async def worker_loop(self):
        """Основной цикл работника с улучшенной обработкой отмены задач"""
        logger.info("🚀 Парсер запущен и ожидает задачи...")
        
        while self.is_running:
            try:
                # Получаем следующую задачу из базы
                task = db.get_pending_task()
                
                if task:
                    task_id = task['id']
                    logger.info(f"📋 Найдена задача #{task_id} для обработки")
                    
                    # Обновляем статус задачи на "обрабатывается"
                    success = db.update_task_status(task_id, 'processing')
                    
                    if not success:
                        logger.warning(f"⚠️ Не удалось обновить статус задачи #{task_id} (возможно, отменена)")
                        await asyncio.sleep(1)
                        continue
                    
                    # Обрабатываем задачу
                    result = await self.process_task(task)
                    
                    # Проверяем, была ли задача отменена
                    if result.get('cancelled', False):
                        logger.info(f"⏹️ Задача #{task_id} была отменена, пропускаем обновление статуса")
                        continue
                    
                    # Обновляем статус задачи в зависимости от результата
                    if result['success']:
                        if result.get('users_found', 0) > 0:
                            success = db.update_task_status(
                                task_id, 
                                'completed',
                                result_filename=result.get('filename'),
                                users_found=result.get('users_found', 0)
                            )
                            if success:
                                logger.info(f"✅ Задача #{task_id} успешно завершена")
                            else:
                                logger.warning(f"⚠️ Задача #{task_id} завершена, но статус не обновлен (возможно, отменена)")
                        else:
                            success = db.update_task_status(
                                task_id, 
                                'completed',
                                result_filename=None,
                                users_found=0
                            )
                            if success:
                                logger.info(f"ℹ️ Задача #{task_id} завершена (нет активных пользователей)")
                    else:
                        error_msg = result.get('error', 'Unknown error')
                        # Не обновляем статус для отмененных задач
                        if 'отменена' not in error_msg.lower():
                            success = db.update_task_status(
                                task_id, 
                                'failed',
                                error_message=error_msg[:100]  # Ограничиваем длину ошибки
                            )
                            if success:
                                logger.error(f"❌ Задача #{task_id} завершилась с ошибкой: {error_msg}")
                        
                        # Если это FloodWait, делаем паузу
                        if 'FloodWait' in error_msg:
                            wait_time = result.get('retry_after', 60)
                            logger.warning(f"⏳ Пауза {wait_time} секунд из-за FloodWait...")
                            await asyncio.sleep(wait_time)
                else:
                    # Нет задач - ждём 5 секунд
                    await asyncio.sleep(5)
                    
            except KeyboardInterrupt:
                logger.info("🛑 Получен сигнал прерывания")
                self.is_running = False
                
            except Exception as e:
                logger.error(f"❌ Критическая ошибка в основном цикле: {e}")
                await asyncio.sleep(30)  # Большая пауза при критической ошибке
    
    async def start(self):
        """Запуск работника"""
        # Инициализируем клиент
        if not await self.initialize_client():
            logger.error("❌ Не удалось инициализировать клиент Telegram")
            return False
        
        logger.info("✅ Парсер готов к работе")
        
        # Запускаем основной цикл
        try:
            await self.worker_loop()
        finally:
            # Закрываем соединение при завершении
            if self.client and self.client.is_connected():
                await self.client.disconnect()
                logger.info("📴 Соединение с Telegram закрыто")
        
        return True

# --- Запуск парсера ---
async def main():
    worker = ParserWorker()
    await worker.start()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Парсер остановлен пользователем")