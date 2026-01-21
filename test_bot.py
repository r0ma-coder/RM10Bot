import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = "8457649746:AAFqlHpszZisrBS21VrMeJrknen6PHtNHHk"  # Замените на токен от @BotFather

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Простой заглушка базы данных для теста
fake_tasks = [
    {"id": 1, "chat_link": "https://t.me/test1", "status": "pending"},
    {"id": 2, "chat_link": "https://t.me/test2", "status": "processing"}
]

@dp.message(Command("tasks"))
async def cmd_tasks(message: types.Message):
    """Тестовая команда /tasks с инлайн-кнопкой"""
    # Текст с задачами
    tasks_text = "📋 <b>Ваши задачи:</b>\n\n"
    for task in fake_tasks:
        icon = "⏳" if task["status"] == "pending" else "🔄"
        tasks_text += f"{icon} <b>Задача #{task['id']}</b>\n"
        tasks_text += f"Ссылка: {task['chat_link']}\n"
        tasks_text += f"Статус: {task['status']}\n"
        tasks_text += "─" * 30 + "\n"
    
    # Клавиатура с ОДНОЙ инлайн-кнопкой
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ Отменить задачу", callback_data="cancel_menu")]
    ])
    
    # Отправляем сообщение с клавиатурой
    await message.answer(tasks_text, reply_markup=keyboard)

@dp.callback_query(F.data == "cancel_menu")
async def cancel_menu(callback: types.CallbackQuery):
    """Обработчик кнопки отмены"""
    # Создаём кнопки с номерами задач
    keyboard_buttons = [
        [InlineKeyboardButton(text=f"Задача #{task['id']}", callback_data=f"cancel_{task['id']}")]
        for task in fake_tasks
    ]
    
    # Добавляем кнопку "Назад"
    keyboard_buttons.append([InlineKeyboardButton(text="↩️ Назад", callback_data="back")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    # Редактируем сообщение
    await callback.message.edit_text(
        "Выберите задачу для отмены:",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data == "back")
async def back_handler(callback: types.CallbackQuery):
    """Возврат к списку задач"""
    await cmd_tasks(callback.message)
    await callback.answer()

async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())