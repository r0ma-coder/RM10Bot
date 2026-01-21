import asyncio
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from database import db

# --- Настройки ---
BOT_TOKEN = "8457649746:AAFqlHpszZisrBS21VrMeJrknen6PHtNHHk"  # Замените на токен от @BotFather

# --- Инициализация ---
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# --- Класс состояний FSM ---
class ParserStates(StatesGroup):
    waiting_for_link = State()
    waiting_for_limit = State()

# --- Обработчики команд ---

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    
    welcome_text = (
        "<b>👋 Привет! Я бот для парсинга активных участников чатов!</b>\n\n"
        "<b>📎 Отправь мне ссылку на публичный чат или канал:</b>\n"
        "• <code>https://t.me/chat_username</code>\n"
        "• <code>@chat_username</code>\n\n"
        "<b>📋 Команды:</b>\n"
        "/tasks - Посмотреть ваши задачи\n"
        "/help - Помощь\n"
        "/cancel - Отменить текущее действие"
    )
    
    await message.answer(welcome_text, reply_markup=ReplyKeyboardRemove())
    await state.set_state(ParserStates.waiting_for_link)

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Действие отменено. Используйте /start чтобы начать заново.")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    help_text = (
        "<b>ℹ️ Помощь по использованию бота:</b>\n\n"
        "1. Отправьте ссылку на публичный чат/канал\n"
        "2. Укажите лимит (0 для максимума 300, или число от 1 до 300)\n"
        "3. Бот добавит задачу в очередь парсинга\n"
        "4. Когда парсинг завершится, бот сохранит результат\n\n"
        "<b>📋 Для просмотра статуса задач используйте /tasks</b>\n\n"
        "<i>Примечание: Парсинг может занять некоторое время в зависимости от размера чата.</i>"
    )
    await message.answer(help_text)

@dp.message(Command("tasks"))
async def cmd_tasks(message: types.Message):
    """Показывает последние задачи пользователя с кнопкой отмены"""
    user_tasks = db.get_user_tasks(message.from_user.id, limit=10)
    
    if not user_tasks:
        await message.answer("📭 <b>У вас пока нет задач.</b>\n\nИспользуйте /start чтобы создать первую задачу.")
        return
    
    tasks_text = "<b>📋 Ваши последние задачи:</b>\n\n"
    
    for task in user_tasks:
        # Иконки статусов
        status_icons = {
            'pending': '⏳',
            'processing': '🔄',
            'completed': '✅',
            'failed': '❌',
            'cancelled': '🚫'
        }
        
        icon = status_icons.get(task['status'], '📌')
        
        # Форматируем время
        created_time = task['created_at'][:19] if task['created_at'] else 'N/A'
        
        tasks_text += f"{icon} <b>Задача #{task['id']}</b>\n"
        tasks_text += f"<code>{task['chat_link'][:30]}</code>\n"
        tasks_text += f"Лимит: <b>{task['limit_count']}</b>\n"
        tasks_text += f"Статус: <b>{task['status']}</b>\n"
        
        if task['status'] == 'completed' and task['users_found'] > 0:
            tasks_text += f"Найдено: <b>{task['users_found']}</b> пользователей\n"
        elif task['status'] == 'failed' and task['error_message']:
            tasks_text += f"Ошибка: <i>{task['error_message'][:50]}</i>\n"
        
        tasks_text += f"Создана: <i>{created_time}</i>\n"
        tasks_text += "─" * 30 + "\n"
    
    tasks_text += f"\n<b>Всего задач:</b> {len(user_tasks)}"
    
    # Создаем клавиатуру с кнопкой отмены
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Отменить задачу", callback_data="cancel_task_menu")]
    ])
    
    await message.answer(tasks_text, reply_markup=keyboard)

@dp.message(ParserStates.waiting_for_link)
async def process_link(message: types.Message, state: FSMContext):
    user_link = message.text.strip()
    
    # Обработка отмены
    if user_link.lower() == '/cancel':
        await cmd_cancel(message, state)
        return
    
    # Валидация ссылки
    if not (user_link.startswith("https://t.me/") or user_link.startswith("@")):
        await message.answer(
            "❌ <b>Неверный формат ссылки!</b>\n\n"
            "Пожалуйста, отправьте ссылку в одном из форматов:\n"
            "• <code>https://t.me/username</code>\n"
            "• <code>@username</code>\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    # Сохраняем ссылку
    await state.update_data(chat_link=user_link)
    
    limit_text = (
        "<b>🔢 Установите ограничение количества юзернеймов:</b>\n\n"
        "• <b>0</b> - без ограничения (максимум 300)\n"
        "• <b>1-300</b> - конкретное количество\n\n"
        "<i>Просто отправьте цифру:</i>"
    )
    
    await message.answer(limit_text)
    await state.set_state(ParserStates.waiting_for_limit)

@dp.message(ParserStates.waiting_for_limit)
async def process_limit(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    
    # Обработка отмены
    if user_input.lower() == '/cancel':
        await cmd_cancel(message, state)
        return
    
    # Проверяем, что введено число
    if not user_input.isdigit():
        await message.answer(
            "❌ <b>Некорректный ввод!</b>\n\n"
            "Пожалуйста, введите только цифру:\n"
            "• <b>0</b> - без ограничения\n"
            "• <b>1-300</b> - конкретное количество\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    limit = int(user_input)
    
    # Проверяем диапазон
    if limit > 300:
        await message.answer(
            "❌ <b>Слишком большое ограничение!</b>\n\n"
            "Максимальное значение: <b>300</b>\n"
            "Введите число от <b>0</b> до <b>300</b>:\n\n"
            "Или отправьте /cancel для отмены."
        )
        return
    
    # Получаем сохранённую ссылку
    data = await state.get_data()
    chat_link = data.get("chat_link")
    
    # Определяем итоговый лимит
    final_limit = 300 if limit == 0 else limit
    limit_message = "без ограничения (максимум 300)" if limit == 0 else f"не более {final_limit}"
    
    # Сохраняем задачу в базу данных
    try:
        task_id = db.create_task(
            user_id=message.from_user.id,
            chat_link=chat_link,
            limit_count=final_limit
        )
        
        result_text = (
            f"✅ <b>Задача #{task_id} создана!</b>\n\n"
            f"📎 <b>Ссылка:</b> <code>{chat_link}</code>\n"
            f"🔢 <b>Ограничение:</b> {limit_message}\n"
            f"👤 <b>Пользователь:</b> {message.from_user.full_name}\n\n"
            "<b>⏳ Задача поставлена в очередь на парсинг.</b>\n"
            "Используйте /tasks для отслеживания статуса."
        )
        
        # Логируем создание задачи
        logging.info(f"User {message.from_user.id} создал задачу #{task_id} для {chat_link}")
        
    except Exception as e:
        logging.error(f"Ошибка при создании задачи: {e}")
        result_text = (
            "❌ <b>Ошибка при создании задачи!</b>\n\n"
            "Попробуйте позже или обратитесь к администратору."
        )
    
    await message.answer(result_text, reply_markup=ReplyKeyboardRemove())
    await state.clear()

@dp.message()
async def handle_other_messages(message: types.Message):
    """Обработка всех остальных сообщений"""
    await message.answer(
        "🤔 <b>Не понял вашу команду.</b>\n\n"
        "Используйте /start чтобы начать создание задачи парсинга.\n"
        "Или /help для получения справки."
    )

# --- Обработчики инлайн-кнопок ---

@dp.callback_query(F.data == "cancel_task_menu")
async def cancel_task_menu(callback: types.CallbackQuery):
    """Показывает меню выбора задачи для отмены"""
    user_tasks = db.get_user_tasks(callback.from_user.id, limit=10)
    
    # Фильтруем задачи, которые можно отменить (только pending и processing)
    cancellable_tasks = [t for t in user_tasks if t['status'] in ['pending', 'processing']]
    
    if not cancellable_tasks:
        await callback.answer("❌ Нет задач, которые можно отменить", show_alert=True)
        return
    
    # Создаем клавиатуру с кнопками для каждой задачи
    keyboard_buttons = []
    
    for task in cancellable_tasks[:10]:  # Максимум 10 задач
        status_icon = '⏳' if task['status'] == 'pending' else '🔄'
        task_text = f"{status_icon} Задача #{task['id']}"
        
        keyboard_buttons.append([
            InlineKeyboardButton(
                text=task_text,
                callback_data=f"cancel_task_{task['id']}"
            )
        ])
    
    # Добавляем кнопку "Назад"
    keyboard_buttons.append([
        InlineKeyboardButton(text="↩️ Назад к списку", callback_data="back_to_tasks")
    ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    # Редактируем сообщение с новым текстом и клавиатурой
    await callback.message.edit_text(
        "🗑️ <b>Выберите задачу для отмены:</b>\n\n"
        "• ⏳ - Ожидает обработки\n"
        "• 🔄 - В процессе обработки\n\n"
        "<i>Отменить можно только задачи в статусе 'ожидает' или 'в процессе'.</i>",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("cancel_task_"))
async def cancel_task_confirm(callback: types.CallbackQuery):
    """Подтверждение отмены задачи"""
    task_id = callback.data.split("_")[-1]
    
    if not task_id.isdigit():
        await callback.answer("Неверный ID задачи", show_alert=True)
        return
    
    # Получаем информацию о задаче
    task_info = db.get_task_info(task_id, callback.from_user.id)
    
    if not task_info:
        await callback.answer("Задача не найдена или у вас нет доступа", show_alert=True)
        return
    
    # Проверяем, можно ли отменить задачу
    if task_info['status'] not in ['pending', 'processing']:
        await callback.answer(f"Невозможно отменить задачу в статусе '{task_info['status']}'", show_alert=True)
        return
    
    # Создаем клавиатуру для подтверждения
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, отменить", callback_data=f"confirm_cancel_{task_id}"),
            InlineKeyboardButton(text="❌ Нет, вернуться", callback_data="back_to_tasks")
        ]
    ])
    
    # Редактируем сообщение с подтверждением
    await callback.message.edit_text(
        f"⚠️ <b>Вы уверены, что хотите отменить задачу #{task_id}?</b>\n\n"
        f"📎 Ссылка: <code>{task_info['chat_link'][:30]}...</code>\n"
        f"📊 Статус: <b>{task_info['status']}</b>\n"
        f"🔢 Лимит: <b>{task_info['limit_count']}</b>\n\n"
        "<i>Это действие нельзя будет отменить.</i>",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("confirm_cancel_"))
async def cancel_task_execute(callback: types.CallbackQuery):
    """Выполняет отмену задачи"""
    task_id = callback.data.split("_")[-1]
    
    if not task_id.isdigit():
        await callback.answer("Неверный ID задачи", show_alert=True)
        return
    
    # Отменяем задачу в базе данных
    success = db.cancel_task(task_id, callback.from_user.id)
    
    if success:
        await callback.message.edit_text(
            f"✅ <b>Задача #{task_id} успешно отменена!</b>\n\n"
            f"Время отмены: <i>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</i>\n\n"
            "Используйте /tasks для просмотра обновленного списка задач."
        )
        
        # Логируем отмену задачи
        logging.info(f"User {callback.from_user.id} отменил задачу #{task_id}")
        await callback.answer(f"Задача #{task_id} отменена")
    else:
        await callback.answer("❌ Ошибка при отмене задачи", show_alert=True)

@dp.callback_query(F.data == "back_to_tasks")
async def back_to_tasks(callback: types.CallbackQuery):
    """Возвращает к списку задач"""
    user_tasks = db.get_user_tasks(callback.from_user.id, limit=10)
    
    if not user_tasks:
        await callback.message.edit_text("📭 <b>У вас пока нет задач.</b>\n\nИспользуйте /start чтобы создать первую задачу.")
        await callback.answer()
        return
    
    tasks_text = "<b>📋 Ваши последние задачи:</b>\n\n"
    
    for task in user_tasks:
        status_icons = {
            'pending': '⏳',
            'processing': '🔄',
            'completed': '✅',
            'failed': '❌',
            'cancelled': '🚫'
        }
        
        icon = status_icons.get(task['status'], '📌')
        created_time = task['created_at'][:19] if task['created_at'] else 'N/A'
        
        tasks_text += f"{icon} <b>Задача #{task['id']}</b>\n"
        tasks_text += f"<code>{task['chat_link'][:30]}</code>\n"
        tasks_text += f"Лимит: <b>{task['limit_count']}</b>\n"
        tasks_text += f"Статус: <b>{task['status']}</b>\n"
        
        if task['status'] == 'completed' and task['users_found'] > 0:
            tasks_text += f"Найдено: <b>{task['users_found']}</b> пользователей\n"
        elif task['status'] == 'failed' and task['error_message']:
            tasks_text += f"Ошибка: <i>{task['error_message'][:50]}</i>\n"
        
        tasks_text += f"Создана: <i>{created_time}</i>\n"
        tasks_text += "─" * 30 + "\n"
    
    tasks_text += f"\n<b>Всего задач:</b> {len(user_tasks)}"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Отменить задачу", callback_data="cancel_task_menu")]
    ])
    
    await callback.message.edit_text(tasks_text, reply_markup=keyboard)
    await callback.answer()

# --- Запуск бота ---
async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Проверяем подключение к базе данных
    try:
        test_tasks = db.get_user_tasks(1, limit=1)
        logging.info("✅ Подключение к базе данных успешно")
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к базе данных: {e}")
        return
    
    logging.info("🚀 Бот запускается...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())