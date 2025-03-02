import logging
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, PreCheckoutQuery, LabeledPrice
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest, TelegramNetworkError
from aiogram.fsm.context import FSMContext
from aiogram.filters.command import Command
from aiogram.fsm.state import State, StatesGroup
from aiogram import Router, F
from threading import Thread
from flask import Flask
import sqlite3
import os
import random
import time
import urllib3
import requests
import asyncio
from datetime import datetime

# Налаштування для уникнення помилок з сертифікатами
urllib3.disable_warnings()

# Конфігураційні константи
API_TOKEN = '7586643431:AAGJYV3WnYETffUVE1Hy8b7xdp7zubS3W88'


ADMIN_ID = 1270564746
REFERRAL_REWARD = 0.2
MIN_WITHDRAWAL = 3.0
router = Router()
channel_router = Router()
logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

@app.route('/')
def keep_alive():
    return "Bot is alive!"


async def notify_admin(bot: Bot, text):
    try:
        safe_text = text.replace('<', '&lt;').replace('>', '&gt;')  # Екранування HTML
        await bot.send_message(ADMIN_ID, safe_text, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Помилка відправки адміну: {e}")

def ensure_database_exists():
    """Функція для перевірки та створення необхідних директорій та бази даних"""
    try:
        if os.path.isfile('bot_database.db'):
            print('✅ База даних існує')
        # Створюємо базу даних, якщо вона не існує
        else:
            # If db not exist init database
            init_db()
        return True
    except Exception as e:
        print(f"❌ Помилка при створенні бази даних: {str(e)}")
        return False

# Клас для станів користувача (використовуємо FSM Aiogram)
class UserState(StatesGroup):
    none = State()  # Замість рядків використовуємо State
    waiting_for_withdrawal = State()
    waiting_for_broadcast = State()
    waiting_for_channel_add = State()
    waiting_for_balance_change = State()
    waiting_promo = State()
    waiting_admin_promo = State()
    waiting_admin_reward = State()
    waiting_admin_activations = State()
    waiting_for_amount = State()
    waiting_for_wallet = State()
    waiting_for_stars_amount = State()
    waiting_for_new_balance = State()

def safe_db_connect():
    """Функція для безпечного підключення до бази даних"""
    try:
        # Спочатку переконуємося, що база даних існує
        if not ensure_database_exists():
            raise Exception("Не вдалося забезпечити існування бази даних")

        conn = sqlite3.connect('bot_database.db', check_same_thread=False)
        return conn
    except sqlite3.Error as e:
        error_message = f"❌ Помилка підключення до БД: {str(e)}"
        print(error_message)
        asyncio.create_task(notify_admin(error_message))
        return None

async def safe_execute_sql(query, params=None, fetch_one=False):
    conn = None
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        print(f"Executing query: {query}")
        print(f"Parameters: {params}")

        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        # Для DELETE/UPDATE повертаємо кількість змінених рядків
        if query.strip().upper().startswith(('DELETE', 'UPDATE')):
            affected_rows = cursor.rowcount
            print(f"Rows affected: {affected_rows}")
            conn.commit()
            return affected_rows

        if fetch_one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()

        print(f"Query result: {result}")
        conn.commit()
        return result

    except Exception as e:
        error_message = f"Database error: {str(e)}"
        print(error_message)
        if conn:
            conn.rollback()
        return None

    finally:
        if conn:
            conn.close()

# Створюємо додаткове поле is_blocked у таблиці users
def update_db_for_user_blocking():
    """Функція для оновлення структури бази даних для підтримки блокування користувачів"""
    conn = sqlite3.connect('bot_database.db')
    try:
        c = conn.cursor()
        # Перевіряємо чи є вже колонка is_blocked
        c.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in c.fetchall()]

        if 'is_blocked' not in columns:
            c.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")
            conn.commit()
            print("✅ Додано колонку is_blocked до таблиці users")
        else:
            print("Колонка is_blocked вже існує")
    except Exception as e:
        print(f"❌ Помилка при оновленні таблиці users: {str(e)}")
    finally:
        conn.close()

def update_db_for_settings():
    conn = sqlite3.connect('bot_database.db')
    try:
        c = conn.cursor()
        c.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in c.fetchall()]
        if 'hide_in_leaderboard' not in columns:
            c.execute("ALTER TABLE users ADD COLUMN hide_in_leaderboard INTEGER DEFAULT 0")
            print("✅ Додано колонку hide_in_leaderboard до таблиці users")
        conn.commit()
    except Exception as e:
        print(f"❌ Помилка при оновленні таблиці users: {str(e)}")
    finally:
        conn.close()

# База даних
def init_db():
    """Функція для ініціалізації бази даних"""
    if os.path.isfile('bot_database.db'):
        print("Database already exists from init function")
        # Додаємо нову колонку is_blocked до існуючої бази даних
        conn = sqlite3.connect('bot_database.db')
        try:
            c = conn.cursor()

            # Перевіряємо чи існує колонка is_blocked
            cursor = conn.execute('PRAGMA table_info(users)')
            columns = [column[1] for column in cursor.fetchall()]

            # Якщо колонки is_blocked немає, додаємо її
            if 'is_blocked' not in columns:
                c.execute('ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0')
                conn.commit()
                print("Додано нову колонку is_blocked")

            conn.commit()
        except sqlite3.Error as e:
            print(f"Помилка при оновленні бази даних: {e}")
            asyncio.create_task(notify_admin(f"❌ Помилка при оновленні бази даних: {str(e)}"))
        finally:
            conn.close()
    else:
        print('❌База даних не існує. Ініціалізація...')
        conn = sqlite3.connect('bot_database.db')
        try:
            c = conn.cursor()

            # Створюємо всі таблиці в правильному порядку
            c.execute('''CREATE TABLE users
                (user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance REAL DEFAULT 0,
                total_earnings REAL DEFAULT 0,
                referrer_id INTEGER,
                join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                state TEXT DEFAULT 'none',
                temp_data TEXT,
                is_blocked INTEGER DEFAULT 0)''')

            c.execute('''CREATE TABLE channels
                (channel_id TEXT PRIMARY KEY,
                channel_name TEXT,
                channel_link TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_required INTEGER DEFAULT 1)''')

            c.execute('''CREATE TABLE transactions
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                type TEXT,
                status TEXT,
                ton_wallet TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id))''')

            c.execute('''CREATE TABLE promo_codes
                (code TEXT PRIMARY KEY,
                reward REAL,
                max_activations INTEGER,
                current_activations INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

            c.execute('''CREATE TABLE used_promo_codes
                (user_id INTEGER,
                promo_code TEXT,
                activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, promo_code),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (promo_code) REFERENCES promo_codes(code))''')

            c.execute('''CREATE TABLE temp_referrals
                (user_id INTEGER PRIMARY KEY,
                referral_code TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id))''')

            c.execute('''CREATE TABLE referral_history
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                referral_user_id INTEGER NOT NULL,
                reward_amount REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referral_user_id) REFERENCES users(user_id))''')

            conn.commit()
            print("✅ Всі таблиці успішно створені")
        except Exception as e:
            print(f"❌ Помилка при створенні таблиць: {str(e)}")
            asyncio.create_task(notify_admin(f"❌ Помилка при створенні таблиць: {str(e)}"))
        finally:
            conn.close()


# Функція для створення таблиць промокодів
def create_promo_codes_table():
    conn = sqlite3.connect('bot_database.db')
    try:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                reward REAL,
                max_activations INTEGER,
                current_activations INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f"❌ Помилка при створенні таблиці промокодів: {str(e)}")
    finally:
        conn.close()


def add_required_channel(channel_id, channel_name, channel_link):
    """Функція для додавання обов'язкового каналу"""
    try:
        # Спочатку перевіряємо, чи існує таблиця channels
        ensure_database_exists()

        conn = sqlite3.connect('bot_database.db')
        c = conn.cursor()

        # Додаємо канал
        c.execute('''
            INSERT INTO channels (channel_id, channel_name, channel_link, is_required)
            VALUES (?, ?, ?, 1)
        ''', (channel_id, channel_name, channel_link))

        conn.commit()
        print(f"✅ Канал {channel_name} успішно додано")
        return True
    except sqlite3.IntegrityError:
        print(f"❌ Канал {channel_id} вже існує в базі даних")
        return False
    except Exception as e:
        print(f"❌ Помилка при додаванні каналу: {str(e)}")
        return False
    finally:
        conn.close()


async def check_subscription(bot: Bot, user_id):
    """Функція для перевірки підписки користувача на канали"""
    try:
        conn = sqlite3.connect('bot_database.db')
        c = conn.cursor()

        # Отримуємо список обов'язкових каналів
        c.execute('SELECT channel_id FROM channels WHERE is_required = 1')
        channels = c.fetchall()
        conn.close()

        if not channels:
            print("Немає обов'язкових каналів для перевірки")
            return True

        for channel in channels:
            try:
                member = await bot.get_chat_member(channel[0], user_id)
                # В aiogram 3 статусы члена чата изменились
                if member.status not in ['member', 'administrator', 'creator', 'owner']:
                    return False
            except TelegramAPIError as e:
                print(f"Помилка перевірки каналу {channel[0]}: {str(e)}")
                continue
        return True
    except Exception as e:
        print(f"Помилка перевірки підписки: {str(e)}")
        return False

def check_users_table(user_id):  # Додаємо параметр user_id
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        print(f"Users table structure:", columns)
        conn.close()
    except Exception as e:
        print(f"Error checking users table: {str(e)}")

    # Виконуємо запит для перевірки
    conn = sqlite3.connect('bot_database.db')
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM users WHERE user_id = ?', (user_id,))
    user_exists = cursor.fetchone()
    conn.close()

    if not user_exists:
        print(f"User {user_id} not found in database")


async def create_main_keyboard(user_id):
    buttons = [
        [types.KeyboardButton(text='💰 Баланс'), types.KeyboardButton(text='👥 Реферальная система')],
        [types.KeyboardButton(text='💳 Вывести деньги'), types.KeyboardButton(text='📊 Моя статистика')],
        [types.KeyboardButton(text='🎮 Мини игры'), types.KeyboardButton(text='🍀 Промокод')],
        [types.KeyboardButton(text='🏆 Таблица лидеров'), types.KeyboardButton(text='Обмен Stars⭐️')],
        [types.KeyboardButton(text='🛠️Тех.Поддержка'), types.KeyboardButton(text='⚙️ Настройки')],
    ]
    if user_id == ADMIN_ID:
        buttons.append([types.KeyboardButton(text='🔑 Адмін панель')])

    keyboard = types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    return keyboard

# Команда старт
@router.message(Command("start"))
async def start(message: Message, state: FSMContext, bot: Bot):
    logging.info(f"Отримано /start від {message.from_user.id}")  # Лог початку обробки
    if message.from_user.is_bot:
        logging.info(f"Ігнорую /start від бота {message.from_user.id}")
        return

    user_id = message.from_user.id
    username = message.from_user.username or "Anonymous"
    logging.info(f"Обробка для user_id={user_id}, username={username}")

    # Зберігаємо реферальний код
    if len(message.text.split()) > 1:
        referral_code = message.text.split()[1]
        logging.info(f"Отримано реферальний код: {referral_code}")
        # Перевіряємо чи не був користувач вже рефералом
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM referral_history WHERE referral_user_id = ?", (user_id,))
        existing_referral = cursor.fetchone()
        logging.info(f"Перевірка referral_history для {user_id}: {existing_referral}")
        conn.close()

        if not existing_referral and referral_code != str(user_id):
            conn = sqlite3.connect('bot_database.db')
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO temp_referrals (user_id, referral_code) VALUES (?, ?)",
                           (user_id, referral_code))
            conn.commit()
            logging.info(f"Додано до temp_referrals: user_id={user_id}, referral_code={referral_code}")
            conn.close()

    try:
        # Перевірка підписки
        logging.info(f"Запуск check_subscription для {user_id}")
        if not await check_subscription(user_id):
            logging.info(f"Користувач {user_id} не пройшов перевірку підписки")
            keyboard = InlineKeyboardMarkup(inline_keyboard=[])
            conn = sqlite3.connect('bot_database.db')
            cursor = conn.cursor()
            cursor.execute('SELECT channel_id, channel_name, channel_link FROM channels WHERE is_required = 1')
            channels = cursor.fetchall()
            logging.info(f"Список каналів для підписки: {channels}")
            conn.close()

            if channels:
                for channel in channels:
                    keyboard.inline_keyboard.append([
                        InlineKeyboardButton(
                            text=f"📢 {channel[1]}",
                            url=channel[2]
                        )
                    ])

                keyboard.inline_keyboard.append([
                    InlineKeyboardButton(
                        text="✅ Проверить подписку",
                        callback_data="check_subscription"
                    )
                ])

                await message.answer("🔔 Для использования бота подпишитесь на наши каналы:",
                                     reply_markup=keyboard)
                logging.info(f"Надіслано повідомлення про підписку для {user_id}")
                return

        # Перевіряємо чи користувач вже існує
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, referrer_id FROM users WHERE user_id = ?", (user_id,))
        existing_user = cursor.fetchone()
        logging.info(f"Перевірка users для {user_id}: {existing_user}")
        conn.close()

        # Отримуємо реферальний код
        referral_code = message.text.split()[1] if len(message.text.split()) > 1 else None
        logging.info(f"Повторне отримання реферального коду: {referral_code}")

        # Перевіряємо чи не був користувач вже рефералом
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM referral_history WHERE referral_user_id = ?", (user_id,))
        existing_referral = cursor.fetchone()
        logging.info(f"Повторна перевірка referral_history для {user_id}: {existing_referral}")
        conn.close()

        if not existing_user and not existing_referral:
            if referral_code and referral_code != str(user_id):
                referrer_id = int(referral_code)
                logging.info(f"Реферер визначений як {referrer_id}")

                # Перевіряємо чи існує реферер
                conn = sqlite3.connect('bot_database.db')
                cursor = conn.cursor()
                cursor.execute("SELECT user_id, balance FROM users WHERE user_id = ?", (referrer_id,))
                referrer = cursor.fetchone()
                logging.info(f"Перевірка реферера {referrer_id}: {referrer}")
                conn.close()

                if referrer:
                    # Додаємо нового користувача з реферером
                    conn = sqlite3.connect('bot_database.db')
                    cursor = conn.cursor()
                    cursor.execute("""INSERT INTO users (user_id, username, referrer_id)
                                   VALUES (?, ?, ?)""", (user_id, username, referrer_id))
                    conn.commit()
                    logging.info(f"Додано нового користувача {user_id} з реферером {referrer_id}")
                    conn.close()

                    # Нараховуємо бонус реферу
                    new_balance = referrer[1] + REFERRAL_REWARD
                    conn = sqlite3.connect('bot_database.db')
                    cursor = conn.cursor()
                    cursor.execute("UPDATE users SET balance = ?, total_earnings = total_earnings + ? WHERE user_id = ?",
                                   (new_balance, REFERRAL_REWARD, referrer_id))
                    conn.commit()
                    logging.info(f"Оновлено баланс реферера {referrer_id}: {new_balance}")
                    conn.close()

                    # Додаємо запис про реферальну транзакцію
                    conn = sqlite3.connect('bot_database.db')
                    cursor = conn.cursor()
                    cursor.execute("""INSERT INTO transactions (user_id, amount, type, status)
                                   VALUES (?, ?, 'referral_reward', 'completed')""", (referrer_id, REFERRAL_REWARD))
                    conn.commit()
                    logging.info(f"Додано транзакцію referral_reward для {referrer_id}")
                    conn.close()

                    # Додаємо запис в історію рефералів
                    conn = sqlite3.connect('bot_database.db')
                    cursor = conn.cursor()
                    cursor.execute("""INSERT INTO referral_history (referrer_id, referral_user_id, reward_amount)
                                   VALUES (?, ?, ?)""", (referrer_id, user_id, REFERRAL_REWARD))
                    conn.commit()
                    logging.info(f"Додано запис до referral_history: referrer={referrer_id}, referral={user_id}")
                    conn.close()

                    # Відправляємо повідомлення реферу
                    await bot.send_message(
                        referrer_id,
                        f"🎉 У вас новый реферал! (@{username})\n"
                        f"💰 Начислено: {REFERRAL_REWARD}$\n"
                        f"💳 Ваш новый баланс: {new_balance}$",
                        parse_mode="HTML"
                    )
                    logging.info(f"Надіслано повідомлення рефереру {referrer_id}")
                else:
                    # Якщо реферер не знайдений, додаємо користувача без реферера
                    conn = sqlite3.connect('bot_database.db')
                    cursor = conn.cursor()
                    cursor.execute("INSERT INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
                    conn.commit()
                    logging.info(f"Додано нового користувача {user_id} без реферера")
                    conn.close()
            else:
                # Додаємо користувача без реферера
                conn = sqlite3.connect('bot_database.db')
                cursor = conn.cursor()
                cursor.execute("INSERT INTO users (user_id, username) VALUES (?, ?)", (user_id, username))
                conn.commit()
                logging.info(f"Додано нового користувача {user_id} без реферера (немає коду або код = user_id)")
                conn.close()

        # Створюємо реферальне посилання
        bot_info = await bot.get_me()
        ref_link = f"https://t.me/{bot_info.username}?start={user_id}"
        logging.info(f"Створено реферальне посилання: {ref_link}")

        # Отримуємо статистику рефералів
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,))
        referrals_count = cursor.fetchone()
        conn.close()
        referrals_count = referrals_count[0] if referrals_count else 0
        logging.info(f"Кількість рефералів для {user_id}: {referrals_count}")

        # Отримуємо загальний заробіток з рефералів
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        cursor.execute("""SELECT COALESCE(SUM(amount), 0) FROM transactions
                       WHERE user_id = ? AND type = 'referral_reward'""", (user_id,))
        total_ref_earnings = cursor.fetchone()
        conn.close()
        total_ref_earnings = total_ref_earnings[0] if total_ref_earnings else 0
        logging.info(f"Загальний заробіток із рефералів для {user_id}: {total_ref_earnings}")

        welcome_message = (
            f"👋 Приветстувуем в боте!\n\n"
            f"💎 За каждого приглашенного друга вы получаете {REFERRAL_REWARD}$!\n\n"
            f"🔥 Возможности:\n"
            f"💰 Зароботок на рефералах\n"
            f"💳 Вывод денег\n"
            f"📊 Статистика"
        )

        keyboard = await create_main_keyboard(user_id)
        await message.answer(welcome_message, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"Надіслано вітальне повідомлення для {user_id}")

    except Exception as e:
        error_msg = f"Ошибка в функции start: {str(e)}"
        print(error_msg)
        logging.error(error_msg)
        await bot.send_message(ADMIN_ID, f"❌ Ошибка регистрации пользователя: {str(e)}", parse_mode="HTML")


@router.callback_query(F.data == "check_subscription")
async def check_subscription_callback(call: CallbackQuery, state: FSMContext, bot: Bot):
    user_id = call.from_user.id

    if await check_subscription(user_id):
        try:
            # Отримуємо реферальний код з бази даних або тимчасового зберігання
            referral_data = await safe_execute_sql(
                "SELECT referral_code FROM temp_referrals WHERE user_id = ?",
                (user_id,),
                fetch_one=True
            )

            if referral_data and referral_data[0]:
                referrer_id = int(referral_data[0])

                # Перевіряємо чи існує реферер
                referrer = await safe_execute_sql(
                    "SELECT user_id, balance FROM users WHERE user_id = ?",
                    (referrer_id,),
                    fetch_one=True
                )

                # Перевіряємо чи не був цей користувач вже рефералом
                existing_referral = await safe_execute_sql(
                    "SELECT id FROM referral_history WHERE referral_user_id = ?",
                    (user_id,),
                    fetch_one=True
                )

                if referrer and not existing_referral:
                    # Нараховуємо бонус реферу
                    new_balance = referrer[1] + REFERRAL_REWARD
                    await safe_execute_sql(
                        "UPDATE users SET balance = ?, total_earnings = total_earnings + ? WHERE user_id = ?",
                        (new_balance, REFERRAL_REWARD, referrer_id)
                    )

                    # Додаємо запис про транзакцію
                    await safe_execute_sql(
                        """INSERT INTO transactions (user_id, amount, type, status)
                           VALUES (?, ?, 'referral_reward', 'completed')""",
                        (referrer_id, REFERRAL_REWARD)
                    )

                    # Зберігаємо інформацію про реферала в історії
                    await safe_execute_sql(
                        """INSERT INTO referral_history (referrer_id, referral_user_id, reward_amount)
                           VALUES (?, ?, ?)""",
                        (referrer_id, user_id, REFERRAL_REWARD)
                    )

                    # Відправляємо повідомлення реферу
                    username = call.from_user.username or f"User{user_id}"
                    await bot.send_message(
                        referrer_id,
                        f"🎉 У вас новый реферал! (@{username})\n"
                        f"💰 Начислено: {REFERRAL_REWARD}$\n"
                        f"💳 Ваш новый баланс: {new_balance}$"
                    )

            # Видаляємо тимчасові дані
            await safe_execute_sql(
                "DELETE FROM temp_referrals WHERE user_id = ?",
                (user_id,)
            )

            try:
                await bot.delete_message(chat_id=call.message.chat.id, message_id=call.message.message_id)
            except TelegramBadRequest:
                # Якщо повідомлення вже видалено, ігноруємо помилку
                pass

            # Створюємо нове повідомлення для старту
            welcome_message = await call.message.answer(
                "✅ Подписка проверена! Добро пожаловать в бот!"
            )

            # Створюємо об'єкт повідомлення для передачі в функцію start
            await start(welcome_message, state)

        except Exception as e:
            print(f"Error in check_subscription_callback: {str(e)}")
            await bot.send_message(ADMIN_ID, f"❌ Ошибка в обработке подписки: {str(e)}")

    else:
        await call.answer(
            "❌ Вы не подписались на все каналы. Проверьте подписку!"
        )

# Перевірка на блокування
async def check_if_blocked(user_id):
    """Перевіряє чи користувач заблокований"""
    result = await safe_execute_sql(
        'SELECT is_blocked FROM users WHERE user_id = ?',
        (user_id,),
        fetch_one=True
    )

    if result and result[0] == 1:
        return True
    return False


# Обробник текстових повідомлень
@router.message(F.text)
async def handle_text(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip()

    # Пропускаємо команди, які починаються з "/"
    if text.startswith('/'):
        logging.info(f"Пропускаю команду '{text}' у handle_text")
        return

    # Базові перевірки
    if message.from_user.is_bot:
        return

    # Перевірка на блокування користувача (крім адміністратора)
    if await check_if_blocked(user_id) and user_id != ADMIN_ID:
        await message.answer("🚫 Ваш аккаунт заблокирован.")
        return

    # Отримуємо поточний стан користувача
    current_state = await state.get_state()
    print(f"Current user state: {current_state}")

    # Якщо користувач у стані очікування промокоду, обробляємо його
    if current_state == UserState.waiting_promo.state:
        await process_promo_activation(message, state, bot)  # Додаємо bot, якщо оновлено
        return

    # Обробка введення промокоду адміністратором
    if current_state == UserState.waiting_admin_promo.state and user_id == ADMIN_ID:
        await process_promo_code_addition(message, state, bot)  # Додаємо bot, якщо оновлено
        return

    # Обробка додавання каналу
    if current_state == UserState.waiting_for_channel_add.state and user_id == ADMIN_ID:
        await process_new_channel(message, state, bot)
        return

    # Обробка стану очікування суми виведення
    if current_state == UserState.waiting_for_amount.state:
        await process_withdrawal_amount(message, state, bot)
        return

    # Обробка стану очікування TON-гаманця
    if current_state == UserState.waiting_for_wallet.state:
        await process_withdrawal_wallet(message, state, bot)
        return

    # Обробка стану очікування суми для обміну Stars
    if current_state == UserState.waiting_for_stars_amount.state:
        await process_custom_amount(message, state, bot)
        return

    # Обробка стану очікування ID для блокування користувача
    if current_state == "waiting_for_user_block" and user_id == ADMIN_ID:
        try:
            user_to_block = int(text)
            await handle_user_block(message, user_to_block, state, bot)
        except ValueError:
            await message.answer("❌ Невірний формат ID користувача. Введіть число.")
        return

    # Обробка стану очікування ID для розблокування користувача
    if current_state == "waiting_for_user_unblock" and user_id == ADMIN_ID:
        try:
            user_to_unblock = int(text)
            await handle_user_unblock(message, user_to_unblock, state, bot)
        except ValueError:
            await message.answer("❌ Невірний формат ID користувача. Введіть число.")
        return

    # Обробка стану очікування ID для видалення користувача
    if current_state == "waiting_for_user_deletion" and user_id == ADMIN_ID:
        try:
            user_to_delete = int(text)
            await handle_user_deletion(message, user_to_delete, state, bot)
        except ValueError:
            await message.answer("❌ Невірний формат ID користувача. Введіть число.")
        return

    # Обробка стану очікування тексту розсилки
    if current_state == UserState.waiting_for_broadcast.state and user_id == ADMIN_ID:
        await save_broadcast_text(message, state, bot)
        return

    # Обробка стану очікування ID для зміни балансу
    if current_state == UserState.waiting_for_balance_change.state and user_id == ADMIN_ID:
        try:
            user_to_change = int(text)
            await process_user_id_for_balance(message, user_to_change, state, bot)  # Додаємо bot, якщо оновлено
        except ValueError:
            await message.answer("❌ Невірний формат ID користувача. Введіть число.")
        return

    # Обробка стану очікування нової суми для зміни балансу
    if current_state == "waiting_for_new_balance" and user_id == ADMIN_ID:
        await process_new_balance(message, state, bot)  # Додаємо bot, якщо оновлено
        return

    # Новий стан для введення ID користувача для інформації
    if current_state == "waiting_for_user_info" and user_id == ADMIN_ID:
        try:
            target_user_id = int(text)
            await show_user_info(message, target_user_id, state, bot)
        except ValueError:
            await message.answer("❌ Неверный формат ID пользователя. Введите число.")
        return

    # Якщо користувач у іншому стані (крім none), пропускаємо обробку команд
    if current_state is not None:
        logging.info(f"Користувач {user_id} у стані {current_state}, пропускаємо handle_text")
        return

    if not await check_subscription(bot, user_id):  # Передаємо bot
        await start(message, state, bot)  # Передаємо bot
        return

    # Спрощений обробник адмін-панелі
    if text == '🔑 Адмін панель' or text == 'Адмін панель':
        if user_id == ADMIN_ID:
            await show_admin_panel(message, bot)  # Передаємо bot
        return

    # Обробка команд управління користувачами
    user_management_commands = [
        '👥 Управління користувачами',
        '🚫 Заблокувати користувача',
        '✅ Розблокувати користувача',
        '❌ Видалити користувача',
        '🔙 Назад до адмін-панелі'
    ]
    if text in user_management_commands and user_id == ADMIN_ID:
        if text == '👥 Управління користувачами':
            await show_user_management(message)
        elif text == '🚫 Заблокувати користувача':
            await start_user_block(message, state, bot)  # Передаємо bot
        elif text == '✅ Розблокувати користувача':
            await start_user_unblock(message, state, bot)  # Передаємо bot
        elif text == '❌ Видалити користувача':
            await start_user_deletion(message, state, bot)  # Передаємо bot
        elif text == '🔙 Назад до адмін-панелі':
            await show_admin_panel(message, bot)  # Передаємо bot
        return

    # Перевіряємо стан очікування ID для операцій з користувачами
    if user_id == ADMIN_ID:
        try:
            user_state = await state.get_state()

            if user_state:
                if user_state == 'waiting_for_user_deletion':
                    try:
                        user_to_delete = int(text)
                        await handle_user_deletion(message, user_to_delete, state)
                        return
                    except ValueError:
                        await message.answer("❌ Невірний формат ID користувача. Спробуйте ще раз.")
                        return
            elif user_state == 'waiting_for_user_block':
                    try:
                        user_to_block = int(text)
                        await handle_user_block(message, user_to_block, state)
                        return
                    except ValueError:
                        await message.answer("❌ Невірний формат ID користувача. Спробуйте ще раз.")
                        return
            elif user_state == 'waiting_for_user_unblock':
                    try:
                        user_to_unblock = int(text)
                        await handle_user_unblock(message, user_to_unblock, state)
                        return
                    except ValueError:
                        await message.answer("❌ Невірний формат ID користувача. Спробуйте ще раз.")
                        return
        except Exception as e:
            print(f"Error checking state: {str(e)}")

    # Обробка інших команд для адміна
    if user_id == ADMIN_ID:
        admin_commands = {
            '📊 Статистика': lambda m, s: show_statistics(m, s),
            '📢 Розсилка': lambda m, s: start_broadcast(m, s, bot),
            '💵 Змінити баланс': lambda m, s: start_balance_change(m, s, bot),
            '📁 Управління каналами': lambda m, s: show_channel_management(m, bot, s),
            '📝 Заявки': lambda m, s: show_withdrawal_requests(m, bot, s),
            '🎫 Статистика промокодів': lambda m, s: show_promo_stats(m, s, bot),
            '➕ Додати промокод': lambda m, s: start_adding_promo(m, s, bot),
            'ℹ️ Информация': lambda m, s: start_user_info(m, s, bot),
            '🔙 Назад': lambda m, s: back_to_main_menu(m, s, bot)
        }
        if text in admin_commands:
            await admin_commands[text](message, state)
            return

    # Обробка звичайних команд користувача
    user_commands = {
        '💰 Баланс': lambda m, s: show_balance(m, bot, s),
        '👥 Реферальная система': lambda m, s: show_referral_system(m, bot, s),
        '💳 Вывести деньги': lambda m, s: start_withdrawal(m, s, bot),
        '📊 Моя статистика': lambda m, s: show_user_statistics(m, s, bot),
        '🎮 Мини игры': lambda m, s: mini_games_menu(m, s),
        '🎰 Слоты': lambda m, s: slots_menu(m, bot, s),
        '↩️ Назад': lambda m, s: return_to_main(m, s),
        '🍀 Промокод': lambda m, s: handle_promo_code(m, s, bot),
        '🏆 Таблица лидеров': lambda m, s: show_leaders_board(m, bot, s),
        'Обмен Stars⭐️': lambda m, s: exchange_stars_command(m, s, bot),
        '⚙️ Настройки': lambda m, s: show_settings_menu(m, bot, s),
        '🛠️Тех.Поддержка': lambda m, s: tech_support(m, bot, s)
    }
    if text in user_commands:
        await user_commands[text](message, state)
        return

# Функція для обробки стану очікування ID користувача для видалення
async def process_user_deletion(message: Message, state: FSMContext):
    await handle_user_deletion(message)

# Функції для звичайних користувачів
async def show_balance(message: Message, bot: Bot, state: FSMContext = None):
    user_id = message.from_user.id
    try:
        # Змінюємо на fetch_one=True, оскільки ми очікуємо один результат
        result = await safe_execute_sql(
            'SELECT balance, total_earnings FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )

        if result:
            balance, total_earnings = result
            response = (
                f"💰 Ваш баланс: {balance:.2f}$\n"
                f"📈 Общий заработок: {total_earnings:.2f}$"
            )
            await message.answer(response)
        else:
            await message.answer("❌ Помилка отримання балансу")
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"❌ Помилка показу балансу для {user_id}: {str(e)}")
        print(f"Error in show_balance: {str(e)}")  # Додаємо логування помилки

async def show_referral_system(message: Message, bot: Bot, state: FSMContext = None):
    user_id = message.from_user.id
    try:
        logging.info(f"Виконується show_referral_system для {user_id}")

        # Отримуємо кількість унікальних рефералів із referral_history
        result = await safe_execute_sql(
            '''SELECT COUNT(DISTINCT referral_user_id) as ref_count,
                      COALESCE(SUM(reward_amount), 0) as ref_earnings
               FROM referral_history
               WHERE referrer_id = ?''',
            (user_id,),
            fetch_one=True
        )

        if result:
            ref_count, ref_earnings = result
        else:
            ref_count, ref_earnings = 0, 0

        bot_info = await bot.get_me()
        bot_username = bot_info.username
        ref_link = f"https://t.me/{bot_username}?start={user_id}"

        # Текст для Telegram Share (з одним посиланням)
        share_text = (
            f"Переходи по ссылке и зарабатывай вмести со мной!💸💰\n "
        )

        # Кодування тексту для URL
        from urllib.parse import quote
        share_url = f"https://t.me/share/url?url={ref_link}&text={quote(share_text)}"

        # Повідомлення зі статистикою та моноширним посиланням у HTML
        response = (
            f"👥 Реферальная система\n\n"
            f"📊 Статистика рефералов:\n"
            f"👤 Количество рефералов: {ref_count}\n"
            f"💰 Заработок с рефералов: {ref_earnings:.2f}$\n"
            f"💵 Награда за нового реферала: {REFERRAL_REWARD}$\n\n"
            f"🔗 Твоя реферальная ссылка:\n<code>{ref_link}</code>"
        )

        # Кнопка для надсилання
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📩 Поделиться", url=share_url)]
        ])

        # Використовуємо parse_mode="HTML" для моноширного формату
        await message.answer(response, reply_markup=keyboard, parse_mode="HTML")
        logging.info(f"Надіслано реферальну інформацію для {user_id}")
    except Exception as e:
        logging.error(f"Помилка в show_referral_system для {user_id}: {str(e)}")
        await bot.send_message(ADMIN_ID, f"❌ Помилка реферальної системи для {user_id}: {str(e)}")


async def create_admin_keyboard(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        '📊 Статистика',
        '📢 Розсилка',
        '💵 Змінити баланс',
        '📁 Управління каналами',
        '👥 Управління користувачами',  # Нова кнопка
        '🎫 Статистика промокодів',
        '➕ Додати промокод',
        '📝 Заявки',
        'ℹ️ Информация',
        '🔙 Назад'
    ]
    keyboard.add(*buttons)
    await message.answer("🔑 Адмін-панель", reply_markup=keyboard)


async def get_user_state(user_id):
    """Отримання стану користувача з бази даних"""
    try:
        result = await safe_execute_sql(
            'SELECT state FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )
        print(f"Got state for user {user_id}: {result[0] if result else None}")  # Додаємо логування
        return result[0] if result else None
    except Exception as e:
        print(f"Error getting user state: {str(e)}")  # Додаємо логування помилок
        return None


async def start_withdrawal(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    try:
        result = await safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )

        if result:
            balance = result[0]

            if balance >= MIN_WITHDRAWAL:
                msg = (
                    f"💳 Вывести деньги\n\n"
                    f"💰 Ваш баланс: {balance:.2f}$\n"
                    f"💵 Минимальная сумма: {MIN_WITHDRAWAL}$\n\n"
                    f"Введите сумму для вывода:"
                )
                await message.answer(msg)
                # Виправлений синтаксис для встановлення стану
                await state.set_state(UserState.waiting_for_amount)
            else:
                await message.answer(
                    f"❌ Недостаточно средств!\n💰 Минимальная сумма: {MIN_WITHDRAWAL}$"
                )
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"❌ Помилка початку виведення для {user_id}: {str(e)}")


# Функція для обробки суми виведення
@router.message(UserState.waiting_for_amount)
async def process_withdrawal_amount(message: Message, bot: Bot, state: FSMContext):
    try:
        amount = float(message.text.strip())
        user_id = message.from_user.id

        result = await safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )

        if not result:
            await message.answer("❌ Ошибка получения баланса!")
            await state.clear()
            return

        balance = result[0]

        if amount < MIN_WITHDRAWAL:
            await message.answer(f"❌ Минимальная сумма для вывода: {MIN_WITHDRAWAL}$")
            await state.clear()
            return

        if amount > balance:
            await message.answer(f"❌ Недостаточно средств! Доступно: {balance:.2f}$")
            await state.clear()
            return

        # Зберігаємо суму в стані
        await state.update_data(withdrawal_amount=amount)

        # Просимо ввести гаманець
        await message.answer("Введите ваш TON кошелек для вывода:")
        await state.set_state(UserState.waiting_for_wallet)
        logging.info(f"User {user_id} entered withdrawal amount: {amount}")

    except ValueError:
        await message.answer("❌ Введите корректную сумму!")
        await state.clear()
    except Exception as e:
        await message.answer("❌ Произошла ошибка!")
        await bot.send_message(ADMIN_ID, f"❌ Помилка обробки суми виведення для {user_id}: {str(e)}")
        await state.clear()

@router.message(UserState.waiting_for_wallet)
async def process_withdrawal_wallet(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    ton_wallet = message.text.strip()

    # Отримуємо збережену суму
    data = await state.get_data()
    amount = data.get('withdrawal_amount')

    try:
        result = await safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )

        if result and result[0]:
            balance = result[0]

            if balance >= amount:
                # Створюємо транзакцію з TON-гаманцем
                await safe_execute_sql(
                    '''INSERT INTO transactions (user_id, amount, type, status, ton_wallet)
                       VALUES (?, ?, 'withdrawal', 'pending', ?)''',
                    (user_id, amount, ton_wallet)
                )

                # Оновлюємо баланс
                await safe_execute_sql(
                    'UPDATE users SET balance = balance - ? WHERE user_id = ?',
                    (amount, user_id)
                )

                # Повідомлення користувачу і адміну
                await message.answer(
                    f"✅ Заявка на вывод {amount:.2f}$ прийнята!"
                )

                admin_msg = (
                    f"💳 Нова заявка на виведення!\n\n"
                    f"👤 Користувач: {user_id}\n"
                    f"💰 Сума: {amount:.2f}$\n"
                    f"🔑 TON кошелек: {ton_wallet}"
                )

                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="✅ Підтвердити",
                            callback_data=f"approve_withdrawal_{user_id}_{amount}"
                        ),
                        InlineKeyboardButton(
                            text="❌ Відхилити",
                            callback_data=f"reject_withdrawal_{user_id}_{amount}"
                        )
                    ]
                ])

                await bot.send_message(ADMIN_ID, admin_msg, reply_markup=keyboard)
            else:
                await message.answer("❌ Недостаточно средств!")
        else:
            await message.answer("❌ Ошибка получения баланса!")

        # Завершуємо стан
        await state.clear()
        logging.info(f"User {user_id} submitted withdrawal request for {amount}$ to {ton_wallet}")
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"❌ Помилка обробки виведення для {user_id}: {str(e)}")
        await state.clear()


# Функції для адміністратора
async def show_admin_panel(message: Message, bot: Bot):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до адмін-панелі.")
        return

    buttons = [
        [types.KeyboardButton(text='📊 Статистика'), types.KeyboardButton(text='📢 Розсилка')],
        [types.KeyboardButton(text='💵 Змінити баланс'), types.KeyboardButton(text='📁 Управління каналами')],
        [types.KeyboardButton(text='👥 Управління користувачами'), types.KeyboardButton(text='🎫 Статистика промокодів')],
        [types.KeyboardButton(text='➕ Додати промокод'), types.KeyboardButton(text='📝 Заявки')],
        [types.KeyboardButton(text='ℹ️ Информация')],
        [types.KeyboardButton(text='🔙 Назад')]
    ]

    keyboard = types.ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True
    )

    await message.answer("🔑 Админ-панель", reply_markup=keyboard)

    await safe_execute_sql(
        'UPDATE users SET state = ? WHERE user_id = ?',
        ('none', ADMIN_ID)
    )

async def show_statistics(message: Message, bot: Bot, state: FSMContext = None):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    try:
        stats = await safe_execute_sql('''
            SELECT
                COUNT(*) as total_users,
                SUM(balance) as total_balance,
                COUNT(DISTINCT referrer_id) as total_referrers,
                (SELECT COUNT(*) FROM transactions WHERE status = 'pending') as pending_withdrawals
            FROM users
        ''')

        if stats and stats[0]:
            total_users, total_balance, total_referrers, pending_withdrawals = stats[0]

            response = (
                f"📊 Загальна статистика:\n\n"
                f"👥 Користувачів: {total_users}\n"
                f"💰 Загальний баланс: {total_balance:.2f}$\n"
                f"👤 Активних рефералів: {total_referrers}\n"
                f"💳 Очікують виведення: {pending_withdrawals}\n\n"
                f"Останні реєстрації:"
            )

            # Отримуємо останні реєстрації
            recent_users = await safe_execute_sql('''
                SELECT user_id, username, join_date
                FROM users
                ORDER BY join_date DESC
                LIMIT 5
            ''')

            if recent_users:
                for user in recent_users:
                    response += f"\n👤 {user[1] or user[0]} - {user[2]}"

            await message.answer(response)
        else:
            await message.answer("❌ Немає даних для статистики.")
    except Exception as e:
        await message.answer(f"❌ Помилка отримання статистики: {str(e)}")
        logging.error(f"Error in show_statistics: {str(e)}")

async def start_broadcast(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Назад", callback_data="back_to_admin")]
    ])

    instruction_msg = await message.answer(
        "📨 Введіть текст розсилки:\n\n"
        "Ви можете форматувати текст безпосередньо в Telegram - "
        "виділіть частину тексту і використовуйте кнопки форматування.",
        reply_markup=markup
    )

    await state.update_data(instruction_msg_id=instruction_msg.message_id)
    await state.set_state(UserState.waiting_for_broadcast)
    logging.info(f"Admin {message.from_user.id} started broadcast process")

async def save_broadcast_text(message: Message, bot: Bot, state: FSMContext):
    """Зберігає текст розсилки для подальшого використання"""
    if message.from_user.id != ADMIN_ID:  # Змінено на !=
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Перевіряємо, чи це команда назад
    if message.text == "/back":
        # Отримуємо ID повідомлення з інструкцією
        data = await state.get_data()
        instruction_msg_id = data.get("instruction_msg_id")

        # Видаляємо повідомлення з інструкцією, якщо воно існує
        if instruction_msg_id:
            await bot.delete_message(message.chat.id, instruction_msg_id)

        # Скидаємо стан і показуємо адмін-панель
        await state.clear()
        await show_admin_panel(message)
        return

    # Зберігаємо ID повідомлення з текстом розсилки
    broadcast_message_id = message.message_id

    # Запитуємо підтвердження
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"confirm_broadcast_{broadcast_message_id}"),
            InlineKeyboardButton(text="❌ Скасувати", callback_data="back_to_admin")
        ]
    ])

    await message.answer(
        f"📨 Підтвердіть розсилку:\n\n"
        f"Ваш текст буде відправлено всім користувачам зі збереженням форматування.",
        reply_markup=markup
    )

    # Скидаємо стан, оскільки далі обробка буде через callback
    await state.clear()
    logging.info(f"Admin {message.from_user.id} saved broadcast text with message_id {broadcast_message_id}")

async def confirm_broadcast(call: CallbackQuery, state: FSMContext):
    """Обробник підтвердження розсилки"""
    if call.from_user.id != ADMIN_ID:  # Змінено на !=
        await call.answer("У вас немає доступу до цього розділу.", show_alert=True)
        return

    try:
        # Отримуємо ID повідомлення з текстом розсилки
        broadcast_message_id = int(call.data.split("_")[2])

        # Повідомляємо про початок розсилки
        progress_msg = await call.message.answer("📨 Розсилка розпочата...")

        # Починаємо розсилку
        await process_broadcast_with_forward(broadcast_message_id, progress_msg.message_id)

        # Видаляємо кнопки підтвердження
        await call.message.edit_reply_markup(reply_markup=None)

        # Відповідаємо на callback
        await call.answer()
        logging.info(f"Admin {call.from_user.id} confirmed broadcast with message_id {broadcast_message_id}")

    except Exception as e:
        await call.answer(
            text=f"Помилка: {str(e)}",
            show_alert=True
        )
        logging.error(f"Error in confirm_broadcast for admin {call.from_user.id}: {str(e)}")

async def process_broadcast_with_forward(broadcast_message_id, progress_msg_id):
    """Виконує розсилку, зберігаючи форматування"""

    users = await safe_execute_sql('SELECT user_id FROM users')

    if not users:
        await bot.edit_message_text(
            text="❌ Немає користувачів для розсилки",
            chat_id=ADMIN_ID,
            message_id=progress_msg_id
        )
        return

    success = 0
    failed = 0

    for user in users:
        try:
            # Копіюємо повідомлення замість простої відправки тексту
            await bot.copy_message(
                chat_id=user[0],
                from_chat_id=ADMIN_ID,
                message_id=broadcast_message_id
            )

            success += 1

            if (success + failed) % 10 == 0:
                await bot.edit_message_text(
                    text=f"📨 Розсилка в процесі...\n✅ Успішно: {success}\n❌ Невдало: {failed}",
                    chat_id=ADMIN_ID,
                    message_id=progress_msg_id
                )

        except Exception as e:
            print(f"Помилка розсилки для користувача {user[0]}: {str(e)}")
            failed += 1
            continue

    # Створюємо кнопку "Назад"
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Назад до адмін-панелі", callback_data="back_to_admin")]
    ])

    # Відправляємо підсумкове повідомлення
    await bot.edit_message_text(
        text=f"📨 Розсилка завершена!\n✅ Успішно: {success}\n❌ Невдало: {failed}",
        chat_id=ADMIN_ID,
        message_id=progress_msg_id,
        reply_markup=markup
    )

async def back_to_admin(call: CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:  # Змінено на !=
        await call.answer("У вас немає доступу до цього розділу.", show_alert=True)
        return

    try:
        # Видаляємо повідомлення з запитом на розсилку
        await call.message.delete()

        # Скидаємо стан FSM
        current_state = await state.get_state()
        if current_state:
            await state.clear()

        # Повертаємося до адмін-панелі
        await show_admin_panel(call.message)

        # Відповідаємо на callback
        await call.answer()
        logging.info(f"Admin {call.from_user.id} cancelled broadcast and returned to admin panel")

    except Exception as e:
        print(f"Помилка при поверненні до адмін-панелі: {str(e)}")
        logging.error(f"Error in back_to_admin for admin {call.from_user.id}: {str(e)}")
        await call.answer("❌ Помилка при скасуванні.", show_alert=True)

async def safe_send_message(chat_id, text, reply_markup=None, parse_mode=None):
    """
    Безпечна відправка повідомлень з обробкою помилок
    """
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
    except Exception as e:
        if "blocked" in str(e) or "kicked" in str(e) or "chat not found" in str(e) or "user is deactivated" in str(e):
            # Користувач заблокував бота
            print(f"User {chat_id} has blocked the bot")
            # Тут можна додати логіку для позначення користувача як неактивного в БД
            await safe_execute_sql(
                "UPDATE users SET state = 'blocked' WHERE user_id = ?",
                (chat_id,)
            )
        else:
            print(f"Error sending message: {e}")
        return None

# Обробник callback-запитів
@router.callback_query()
async def handle_callback_query(call: CallbackQuery, state: FSMContext):
    try:
        logging.info(f"Callback received: {call.data} from user {call.from_user.id}")
        user_id = call.from_user.id
        current_state = await get_user_state(user_id)
        logging.info(f"User state in callback: {current_state}")

        if "check_subscription" in call.data:
            await check_user_subscription(call)
        elif "approve_withdrawal" in call.data:
            await handle_withdrawal_approval(call)
        elif "reject_withdrawal" in call.data:
            await handle_withdrawal_rejection(call)
        elif call.data == "add_new_channel":
            await start_adding_channel(call.message, state)
        elif call.data.startswith("channel_info_"):
            await show_channel_info(call, state)
        elif call.data == "manage_channels":
            await manage_channels_callback(call)
        elif call.data.startswith("toggle_required_"):
            await toggle_channel_required(call)
        elif call.data.startswith("delete_channel_"):
            await confirm_delete_channel(call)
        elif call.data.startswith("confirm_delete_"):
            await delete_channel(call)
        elif call.data == "back_to_admin":
            await back_to_admin(call, state)
        elif call.data.startswith("confirm_broadcast_"):
            await confirm_broadcast(call, state)
        elif call.data.startswith("spin_slots") or call.data == "exit_slots":
            await handle_slots_callbacks(call, state)
        elif call.data == "toggle_hide_leaderboard":
            await toggle_hide_leaderboard(call)
        # Обробка обміну на Stars
        elif call.data.startswith("exchange_"):
            amount = call.data.split("_")[1]
            if amount == "custom":
                await call.message.edit_text(
                    "Введите количество Stars⭐️ для обмена (мин. 1⭐️):"
                )
                await state.set_state(UserState.waiting_for_stars_amount)
            else:
                stars = int(amount)
                dollars = stars * 3 / 100  # 100⭐️ = 3$
                payload = f"exchange_{stars}_{call.from_user.id}"
                logging.info(f"Создан инвойс для {user_id}: {stars}⭐️ -> {dollars}$ (через кнопку)")
                invoice_msg = await bot.send_invoice(
                    chat_id=call.from_user.id,
                    title=f"Обмен {stars}⭐️",
                    description=f"Обменяйте {stars} Telegram Stars на {dollars}$",
                    payload=payload,
                    currency="XTR",
                    prices=[LabeledPrice(label=f"{stars} Stars", amount=stars)]
                )
                await bot.send_message(
                    chat_id=call.from_user.id,
                    text="Нажмите ниже, чтобы отменить обмен:",
                    reply_markup=get_cancel_keyboard(payload)
                )
                await state.update_data(invoice_msg_id=invoice_msg.message_id)
                await call.message.delete()
        # Обробка скасування обміну
        elif call.data.startswith("cancel_"):
            data = await state.get_data()
            invoice_msg_id = data.get("invoice_msg_id")
            if invoice_msg_id:
                try:
                    await bot.delete_message(chat_id=user_id, message_id=invoice_msg_id)
                    logging.info(f"Инвойс {invoice_msg_id} удален для {user_id}")
                except TelegramAPIError as e:
                    logging.warning(f"Не удалось удалить инвойс {invoice_msg_id}: {str(e)}")
            await call.message.delete()
            await bot.send_message(
                call.from_user.id,
                "❌ Обмен отменен.",
                reply_markup=await create_main_keyboard(user_id)
            )
            await state.clear()
            logging.info(f"Пользователь {user_id} отменил обмен")
        elif call.data == "back_to_main":
            await call.message.delete()
            await bot.send_message(
                call.from_user.id,
                "Вы вернулись в главное меню!",
                reply_markup=await create_main_keyboard(user_id)
            )
            await state.clear()
        elif call.data == "cancel_payment":  # Стара логіка, можливо, не потрібна
            await call.message.edit_text(
                "Обмен отклонен.",
                reply_markup=await create_main_keyboard(user_id)
            )
            await state.clear()
        else:
            logging.info(f"Unhandled callback: {call.data}")
            await call.answer("Это действие не поддерживается.")

        await call.answer()

    except Exception as e:
        logging.error(f"Error in callback handler: {e}")
        await call.answer("❌ Возникла ошибка, обратитесь в тех.поддержку.", show_alert=True)

@router.callback_query(F.data.in_({"spin_slots", "exit_slots"}))
async def handle_slots_callbacks(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logging.info(f"Callback {callback.data} отримано від {user_id}")

    if callback.data == "spin_slots":
        # Перевірка балансу
        balance_result = await safe_execute_sql(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,),
            fetch_one=True
        )

        if balance_result is None or balance_result[0] < 1:
            await callback.answer(
                "❌ Недостаточно средств! Минимальная сумма: 1$",
                show_alert=True
            )
            return

        # Знімаємо 1$
        await safe_execute_sql(
            "UPDATE users SET balance = balance - 1 WHERE user_id = ?",
            (user_id,)
        )

        # Симуляція обертання
        await callback.message.edit_text("🎲 Крутим...")
        await asyncio.sleep(2)

        # Визначаємо результат (45% шанс на виграш)
        win = random.random() < 0.45
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎰 Крутить - 1$", callback_data="spin_slots")],
            [InlineKeyboardButton(text="↩️ Выйти", callback_data="exit_slots")]
        ])

        if win:
            win_amount = 2
            await safe_execute_sql(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (win_amount, user_id)
            )
            await callback.message.edit_text(
                "🎰\n\n🎉 Выпало 3 одинаковых символа! Вы выиграли 2$!",
                reply_markup=keyboard
            )
            logging.info(f"Користувач {user_id} виграв 2$ у слотах")
        else:
            await callback.message.edit_text(
                "🎰\n\n😔 Разные символы. Попробуйте еще раз!",
                reply_markup=keyboard
            )
            logging.info(f"Користувач {user_id} програв у слотах")

    elif callback.data == "exit_slots":
        await callback.message.delete()
        await callback.message.answer(
            "👋 Спасибо за игру!",
            reply_markup=await create_main_keyboard(user_id)
        )
        logging.info(f"Користувач {user_id} вийшов із слотів")

    await callback.answer()

async def handle_withdrawal_approval(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цієї дії.", show_alert=True)
        return

    try:
        _, target_user_id, amount = call.data.split('_')[1:]
        target_user_id = int(target_user_id)
        amount = float(amount)
        logging.info(f"Admin {user_id} approving withdrawal of {amount}$ for user {target_user_id}")

        # Отримуємо ID останньої транзакції зі статусом 'pending'
        transaction_id = await safe_execute_sql(
            '''SELECT id FROM transactions
               WHERE user_id = ? AND amount = ? AND status = 'pending'
               ORDER BY created_at DESC LIMIT 1''',
            (target_user_id, amount),
            fetch_one=True
        )

        if transaction_id:
            # Оновлюємо статус транзакції за знайденим ID
            result = await safe_execute_sql(
                '''UPDATE transactions
                   SET status = 'completed'
                   WHERE id = ?''',
                (transaction_id[0],)
            )

            if result:
                await call.message.edit_text(
                    f"✅ Виведення {amount}$ для користувача {target_user_id} підтверджено!"
                )
                await bot.send_message(
                    target_user_id,
                    f"✅ Ваша заявка на вывод {amount}$ подтверджена!"
                )
            else:
                await call.answer("❌ Не вдалося оновити статус транзакції.", show_alert=True)
                logging.error(f"Failed to update transaction status for id {transaction_id[0]}")
        else:
            await call.answer("❌ Не знайдено активної заявки на виведення.", show_alert=True)
            logging.error(f"No pending transaction found for user {target_user_id}, amount {amount}")

    except Exception as e:
        logging.error(f"Error in handle_withdrawal_approval: {str(e)}")
        await bot.send_message(ADMIN_ID, f"❌ Помилка підтвердження виведення: {str(e)}")
        await call.answer("❌ Помилка при підтвердженні.", show_alert=True)

async def handle_withdrawal_rejection(call: CallbackQuery):
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цієї дії.", show_alert=True)
        return

    try:
        _, target_user_id, amount = call.data.split('_')[1:]
        target_user_id = int(target_user_id)
        amount = float(amount)
        logging.info(f"Admin {user_id} rejecting withdrawal of {amount}$ for user {target_user_id}")

        # Отримуємо ID останньої транзакції зі статусом 'pending'
        transaction_id = await safe_execute_sql(
            '''SELECT id FROM transactions
               WHERE user_id = ? AND amount = ? AND status = 'pending'
               ORDER BY created_at DESC LIMIT 1''',
            (target_user_id, amount),
            fetch_one=True
        )

        if transaction_id:
            # Оновлюємо статус транзакції за знайденим ID
            result = await safe_execute_sql(
                '''UPDATE transactions
                   SET status = 'rejected'
                   WHERE id = ?''',
                (transaction_id[0],)
            )

            if result:
                # Повертаємо кошти на баланс користувача
                await safe_execute_sql(
                    'UPDATE users SET balance = balance + ? WHERE user_id = ?',
                    (amount, target_user_id)
                )

                await call.message.edit_text(
                    f"❌ Виведення {amount}$ для користувача {target_user_id} відхилено!"
                )
                await bot.send_message(
                    target_user_id,
                    f"❌ Ваша заявка на вывод {amount}$ была отклонена.\n💰 Средства возвращены на баланс."
                )
            else:
                await call.answer("❌ Не вдалося оновити статус транзакції.", show_alert=True)
                logging.error(f"Failed to update transaction status for id {transaction_id[0]}")
        else:
            await call.answer("❌ Не знайдено активної заявки на виведення.", show_alert=True)
            logging.error(f"No pending transaction found for user {target_user_id}, amount {amount}")

    except Exception as e:
        logging.error(f"Error in handle_withdrawal_rejection: {str(e)}")
        await bot.send_message(ADMIN_ID, f"❌ Помилка відхилення виведення: {str(e)}")
        await call.answer("❌ Помилка при відхиленні.", show_alert=True)

async def show_user_statistics(message: Message, state: FSMContext = None):
    user_id = message.from_user.id
    try:
        # Отримуємо кількість рефералів із referral_history
        referrals_count_result = await safe_execute_sql(
            '''SELECT COUNT(DISTINCT referral_user_id) FROM referral_history WHERE referrer_id = ?''',
            (user_id,),
            fetch_one=True
        )
        referrals_count = referrals_count_result[0] if referrals_count_result else 0

        # Отримуємо заробіток з рефералів
        ref_earnings_result = await safe_execute_sql(
            '''SELECT COALESCE(SUM(amount), 0) FROM transactions
               WHERE user_id = ? AND type = 'referral_reward' ''',
            (user_id,),
            fetch_one=True
        )
        ref_earnings = ref_earnings_result[0] if ref_earnings_result else 0

        # Отримуємо дату приєднання
        join_date_result = await safe_execute_sql(
            '''SELECT join_date FROM users WHERE user_id = ?''',
            (user_id,),
            fetch_one=True
        )
        join_date = join_date_result[0] if join_date_result else None

        if not join_date:
            raise Exception("Не вдалося отримати дату приєднання")

        # Розраховуємо кількість днів у боті
        join_datetime = datetime.strptime(join_date, '%Y-%m-%d %H:%M:%S')
        days_in_bot = (datetime.now() - join_datetime).days

        # Формуємо повідомлення з жирним заголовком
        response = (
            f"<b>📊 Ваша статистика:</b>\n\n"
            f"👥 Количество рефералов: {referrals_count}\n"
            f"💰 Заработок с рефералов: {ref_earnings:.2f}$\n"
            f"⏳ Дней в боте: {days_in_bot}"
        )

        await message.answer(response, parse_mode="HTML")

    except Exception as e:
        print(f"Помилка у функції статистики: {str(e)}")
        await bot.send_message(ADMIN_ID, f"❌ Помилка показу статистики для {user_id}: {str(e)}")
        await message.answer("❌ Произошла ошибка при получении статистики", parse_mode="HTML")

async def back_to_main_menu(message: Message, state: FSMContext = None):
    user_id = message.from_user.id

    # Створюємо список кнопок
    buttons = [
        [types.KeyboardButton(text='💰 Баланс'), types.KeyboardButton(text='👥 Реферальная система')],
        [types.KeyboardButton(text='💳 Вывести деньги'), types.KeyboardButton(text='📊 Моя статистика')],
        [types.KeyboardButton(text='🎮 Мини игры'), types.KeyboardButton(text='🍀 Промокод')],
        [types.KeyboardButton(text='🏆 Таблица лидеров'), types.KeyboardButton(text='Обмен Stars⭐️')],
        [types.KeyboardButton(text='⚙️ Настройки')],
        [types.KeyboardButton(text='🛠️Тех.Поддержка')]
    ]

    # Додаємо кнопку адмін-панелі для адміністратора
    if user_id == ADMIN_ID:
        buttons.append([types.KeyboardButton(text='🔑 Адмін панель')])

    # Ініціалізуємо ReplyKeyboardMarkup зі списком кнопок
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=buttons,
        resize_keyboard=True,
        row_width=2
    )

    await message.answer("📱 Головне меню:", reply_markup=keyboard)


# Функції для обробки платіжних систем
async def start_balance_change(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    await bot.send_message(
        ADMIN_ID,
        "👤 Введіть ID користувача:"
    )
    await state.set_state(UserState.waiting_for_balance_change)
    logging.info(f"Admin {message.from_user.id} started balance change process")


async def process_user_id_for_balance(message: Message, user_id_to_change: int, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    result = await safe_execute_sql(
        'SELECT balance FROM users WHERE user_id = ?',
        (user_id_to_change,),
        fetch_one=True
    )

    if result:
        current_balance = result[0]
        msg = (
            f"👤 Користувач: {user_id_to_change}\n"
            f"💰 Поточний баланс: {current_balance}$\n\n"
            f"Введіть нову суму балансу:"
        )
        await bot.send_message(ADMIN_ID, msg)
        await state.update_data(user_id=user_id_to_change)
        await state.set_state("waiting_for_new_balance")  # Новий стан для введення суми
        logging.info(f"Admin {message.from_user.id} entered ID {user_id_to_change} for balance change")
    else:
        await bot.send_message(ADMIN_ID, "❌ Користувача не знайдено!")
        await state.clear()

async def process_new_balance(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    try:
        new_balance = float(message.text)
        state_data = await state.get_data()
        user_id = state_data.get("user_id")

        if new_balance < 0:
            await bot.send_message(ADMIN_ID, "❌ Баланс не може бути від'ємним!")
            await state.clear()
            return

        result = await safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )

        if result:
            old_balance = result[0]
            await safe_execute_sql(
                'UPDATE users SET balance = ? WHERE user_id = ?',
                (new_balance, user_id)
            )

            amount_change = new_balance - old_balance
            transaction_type = 'bonus' if amount_change > 0 else 'penalty'
            await safe_execute_sql(
                '''INSERT INTO transactions (user_id, amount, type, status)
                   VALUES (?, ?, ?, 'completed')''',
                (user_id, abs(amount_change), transaction_type)
            )

            admin_msg = (
                f"✅ Баланс користувача {user_id} змінено:\n"
                f"Було: {old_balance}$\n"
                f"Стало: {new_balance}$\n"
                f"Різниця: {amount_change:+}$"
            )
            await bot.send_message(ADMIN_ID, admin_msg)

            user_msg = (
                f"💰 Ваш баланс изменен:\n"
                f"Было: {old_balance}$\n"
                f"Стало: {new_balance}$\n"
                f"Разница: {amount_change:+}$"
            )
            await bot.send_message(user_id, user_msg)
        else:
            await bot.send_message(ADMIN_ID, "❌ Користувача не знайдено!")

        await state.clear()
        logging.info(f"Admin {message.from_user.id} changed balance for user {user_id} to {new_balance}")
    except ValueError:
        await bot.send_message(ADMIN_ID, "❌ Введіть коректну суму!")
        await state.clear()
    except Exception as e:
        await bot.send_message(ADMIN_ID, f"❌ Помилка зміни балансу: {str(e)}")
        await state.clear()


# Функції для роботи з промокодами
@router.message(Command("add_promo"))
async def start_adding_promo(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    await message.answer("Введіть промокод, суму винагороди та максимальну кількість активацій через пробіл\nНаприклад: HAPPY2024 100 50")
    await state.set_state(UserState.waiting_admin_promo)

@router.message(UserState.waiting_admin_promo)
async def process_promo_code_addition(message: Message, state: FSMContext):
    try:
        promo_code, reward, max_activations = message.text.split()
        reward = float(reward)
        max_activations = int(max_activations)

        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        # Додаємо промокод в базу даних
        cursor.execute(
            'INSERT INTO promo_codes (code, reward, max_activations, current_activations) VALUES (?, ?, ?, 0)',
            (promo_code, reward, max_activations)
        )
        conn.commit()
        conn.close()

        await message.answer(f"✅ Промокод успішно додано!\nКод: {promo_code}\nСума: {reward}\nМакс. активацій: {max_activations}")
        await state.clear()
    except ValueError:
        await message.answer("❌ Неправильний формат! Спробуйте ще раз.")
        await state.clear()
    except Exception as e:
        await message.answer(f"❌ Помилка: {str(e)}")
        await state.clear()

async def add_promo_code(code, reward, max_activations):
    await safe_execute_sql(
        'INSERT INTO promo_codes (code, reward, max_activations, current_activations) VALUES (?, ?, ?, 0)',
        (code, reward, max_activations)
    )

@router.message(F.text == "🍀 Промокод")
async def handle_promo_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await message.answer("Введите промокод:")
    await state.set_state(UserState.waiting_promo)
    logging.info(f"Користувач {user_id} увійшов у стан очікування промокоду (UserState.waiting_promo)")

@router.message(UserState.waiting_promo)
async def process_promo_activation(message: Message, bot: Bot, state: FSMContext):
    user_id = message.from_user.id
    promo_code = message.text.strip().upper()

    logging.info(f"Користувач {user_id} у стані waiting_promo ввів: {promo_code}")

    try:
        # Перевірка наявності промокоду
        promo = await safe_execute_sql(
            "SELECT code, reward, max_activations, current_activations FROM promo_codes WHERE code = ?",
            (promo_code,),
            fetch_one=True
        )

        if not promo:
            await message.answer("❌ Неправильный промокод!")
            logging.info(f"Користувач {user_id} ввів неправильний промокод: {promo_code}")
            await state.clear()
            return

        code, reward, max_activations, current_activations = promo
        if current_activations >= max_activations:
            await message.answer("❌ Промокод больше не действителен (достигнут лимит активаций)!")
            logging.info(f"Промокод {promo_code} вичерпав ліміт активацій для {user_id}")
            await state.clear()
            return

        # Перевірка, чи користувач вже використовував цей промокод
        used = await safe_execute_sql(
            "SELECT 1 FROM used_promo_codes WHERE user_id = ? AND promo_code = ?",
            (user_id, promo_code),
            fetch_one=True
        )

        if used:
            await message.answer("❌ Вы уже использовали этот промокод!")
            logging.info(f"Користувач {user_id} вже використав промокод {promo_code}")
            await state.clear()
            return

        # Додавання запису про використання промокоду
        await safe_execute_sql(
            "INSERT INTO used_promo_codes (user_id, promo_code) VALUES (?, ?)",
            (user_id, promo_code)
        )

        # Оновлення кількості активацій промокоду
        await safe_execute_sql(
            "UPDATE promo_codes SET current_activations = current_activations + 1 WHERE code = ?",
            (promo_code,)
        )

        # Отримання поточного балансу користувача
        current_balance_result = await safe_execute_sql(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,),
            fetch_one=True
        )
        current_balance = current_balance_result[0] if current_balance_result else 0

        # Оновлення балансу користувача
        new_balance = current_balance + reward
        await safe_execute_sql(
            "UPDATE users SET balance = ? WHERE user_id = ?",
            (new_balance, user_id)
        )

        await message.answer(
            f"✅ Промокод успешно активирован!\n💰 Начислено: {reward} $\n💳 Ваш баланс: {new_balance} $"
        )
        logging.info(f"Промокод {promo_code} успішно активовано для {user_id}, новий баланс: {new_balance}")

    except Exception as e:
        logging.error(f"Помилка при активації промокоду для {user_id}: {str(e)}")
        await message.answer("❌ Произошла ошибка при активации промокода. Попробуйте позже.")
    finally:
        await state.clear()


async def show_promo_stats(message: Message, bot: Bot, state: FSMContext = None):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Перевіряємо та видаляємо використані промокоди
    await remove_expired_promo_codes()  # Додано await

    try:
        promo_stats = await safe_execute_sql(
            '''
            SELECT
                code,
                reward,
                max_activations,
                current_activations,
                created_at
            FROM promo_codes
            ORDER BY created_at DESC
            ''',
            fetch_one=False
        )

        if not promo_stats:
            await bot.send_message(user_id, "📊 Промокодов еще не создано")
            return

        response = "📊 Статистика промокодов:\n\n"
        for promo in promo_stats:
            code, reward, max_act, curr_act, created_at = promo
            status = "✅ Активен" if curr_act < max_act else "❌ Использован"
            response += (
                f"🎫 Код: {code}\n"
                f"💰 Сумма: {reward} $\n"
                f"📊 Активаций: {curr_act}/{max_act}\n"
                f"📅 Создан: {created_at}\n"
                f"📍 Статус: {status}\n"
                f"➖➖➖➖➖➖➖➖\n"
            )

        await bot.send_message(user_id, response)

    except Exception as e:
        await bot.send_message(user_id, "❌ Ошибка при получении статистики промокодов")
        logging.error(f"Ошибка show_promo_stats: {str(e)}")

# Нова функція для перевірки та видалення старих промокодів
async def remove_expired_promo_codes():  # Змінено на async def
    try:
        # Логуємо промокоди перед видаленням
        promo_codes = await safe_execute_sql(  # Додано await
            '''
            SELECT code, current_activations, max_activations
            FROM promo_codes
            WHERE current_activations >= max_activations
            ''',
            fetch_one=False
        )
        logging.info(f"Промокоди для видалення: {promo_codes}")

        # Виконуємо видалення
        affected_rows = await safe_execute_sql(  # Додано await
            '''
            DELETE FROM promo_codes
            WHERE current_activations >= max_activations
            ''',
            fetch_one=False
        )
        logging.info(f"Видалено {affected_rows} використаних промокодів")
    except Exception as e:
        logging.error(f"Помилка при видаленні використаних промокодів: {str(e)}")

# Хендлер для таблиці лідерів
@router.message(F.text == "🏆 Таблица лидеров")
async def show_leaders_board(message: Message, bot: Bot, state: FSMContext = None):
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                u1.user_id,
                u1.username,
                COUNT(DISTINCT rh.referral_user_id) as referral_count,
                u1.balance,
                u1.hide_in_leaderboard
            FROM
                users u1
                LEFT JOIN referral_history rh ON u1.user_id = rh.referrer_id
            WHERE
                u1.user_id NOT IN (1270564746, 1115356913)
            GROUP BY
                u1.user_id, u1.username, u1.balance, u1.hide_in_leaderboard
            ORDER BY
                referral_count DESC
            LIMIT 10
        ''')

        leaders = cursor.fetchall()
        logging.info(f"Leaders fetched: {leaders}")
        conn.close()

        if leaders:
            # Заголовок із жирним шрифтом через HTML
            response = "<b>🏆 Топ-10 лидеров по количеству рефералов:</b>\n\n"
            for index, (user_id, username, referral_count, balance, hide) in enumerate(leaders, 1):
                display_name = "🙈 Скрыто" if hide else (username if username else f"User{user_id}")
                response += (
                    f"<b>{index}</b>. <b>{display_name}</b>\n"
                    f"   👥 Рефералы: {referral_count}\n"
                    f"   💸 Баланс: ${balance:.2f}\n\n"
                )
        else:
            response = "🏆 Пока нет лидеров. Приглашайте друзей!"

        # Відправляємо повідомлення з HTML-розміткою
        await message.answer(response, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка при показе таблицы лидеров: {str(e)}")
        await message.answer("❌ Не удалось показать таблицу лидеров. Попробуйте позже.", parse_mode="HTML")
        await notify_admin(f"Помилка в таблиці лідерів: {str(e)}")

# Хендлер для технічної підтримки
@router.message(F.text == '🛠️Тех.Поддержка')
async def tech_support(message: Message, bot: Bot, state: FSMContext = None):
    support_link = "tg://resolve?domain=m1sepf"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📞 Написать поддержке", url=support_link)]
    ])
    await message.answer("Перенаправляем к поддержке...", reply_markup=keyboard)

# Хендлер для меню міні-ігор
async def mini_games_menu(message: Message, state: FSMContext = None):
    buttons = [
        [types.KeyboardButton(text='🎰 Слоты')],
        [types.KeyboardButton(text='↩️ Назад')]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
    await message.answer("🎮 Выберите мини-игру:", reply_markup=keyboard)

# Хендлер для меню слотів
@router.message(F.text == "🎰 Слоты")
async def slots_menu(message: Message, bot: Bot, state: FSMContext = None):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎰 Крутить - 1$", callback_data="spin_slots")],
        [InlineKeyboardButton(text="↩️ Выйти", callback_data="exit_slots")]
    ])

    text = (
        "🎰 Игровые автоматы\n\n"
        "💰 Стоимость одного вращения: 1$\n"
        "🏆 Выигрыш при 3 одинаковых символах: 2$\n"
        "Удачи! 🍀"  # Без MarkdownV2 екранування не потрібне
    )

    await message.answer(
        text,
        reply_markup=keyboard
        # Видалено parse_mode="MarkdownV2"
    )

# Хендлер для повернення в головне меню
@router.message(F.text == "↩️ Назад")
async def return_to_main(message: Message, state: FSMContext = None):
    await message.answer(
        "Вы вернулись в главное меню",
        reply_markup=await create_main_keyboard(message.from_user.id)
    )

# Функція для отримання кількості щоденних ігор
async def get_daily_plays(user_id):
    today = datetime.now().date()
    result = await safe_execute_sql(
        """SELECT COUNT(*) FROM transactions
           WHERE user_id = ? AND type = 'slots_game'
           AND DATE(created_at) = ?""",
        (user_id, today),
        fetch_one=True
    )
    return result[0]

# Функція для показу списку заявок
async def show_withdrawal_requests(message: Message, bot: Bot, state: FSMContext = None):
    if message.from_user.id != ADMIN_ID:  # Змінено на !=
        await message.answer("У вас немає доступу до цього розділу.")
        return

    try:
        requests = await safe_execute_sql(
            '''SELECT
                t.user_id,
                t.amount,
                t.ton_wallet,
                t.created_at,
                u.username
               FROM transactions t
               LEFT JOIN users u ON t.user_id = u.user_id
               WHERE t.type = 'withdrawal'
               AND t.status = 'pending'
               ORDER BY t.created_at DESC'''
        )

        if not requests or len(requests) == 0:
            await message.answer("📝 Активних заявок на виведення немає.")
            return

        for req in requests:
            user_id, amount, wallet, created_at, username = req
            username = username if username else f"User {user_id}"

            request_msg = (
                f"💳 Заявка на виведення\n\n"
                f"👤 Користувач: {username}\n"
                f"🆔 ID: {user_id}\n"
                f"💰 Сума: {amount:.2f}$\n"
                f"🔑 TON гаманець: {wallet}\n"
                f"📅 Створено: {created_at}"
            )

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="✅ Підтвердити",
                        callback_data=f"approve_withdrawal_{user_id}_{amount}"
                    ),
                    InlineKeyboardButton(
                        text="❌ Відхилити",
                        callback_data=f"reject_withdrawal_{user_id}_{amount}"
                    )
                ]
            ])

            await message.bot.send_message(
                ADMIN_ID,
                request_msg,
                reply_markup=keyboard
            )
            logging.info(f"Показана заявка на виведення для {user_id}, сума: {amount}")

    except Exception as e:
        error_msg = f"❌ Помилка при отриманні заявок: {str(e)}"
        print(error_msg)
        await message.bot.send_message(ADMIN_ID, error_msg)
        logging.error(error_msg)

# Функція для відображення меню управління користувачами
async def show_user_management(message: Message):
    if message.from_user.id != ADMIN_ID:
        return

    buttons = [
        [types.KeyboardButton(text='🚫 Заблокувати користувача')],
        [types.KeyboardButton(text='✅ Розблокувати користувача')],
        [types.KeyboardButton(text='❌ Видалити користувача')],
        [types.KeyboardButton(text='🔙 Назад до адмін-панелі')]
    ]
    keyboard = types.ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

    await message.answer("👥 Меню управління користувачами", reply_markup=keyboard)

    # Скидаємо стан адміна
    await safe_execute_sql(
        'UPDATE users SET state = ? WHERE user_id = ?',
        ('none', ADMIN_ID)
    )

# Функції для блокування користувачів
async def start_user_block(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return
    await message.answer("Введіть ID користувача, якого бажаєте заблокувати:")
    await state.set_state("waiting_for_user_block")  # Встановлюємо стан через FSMContext
    logging.info(f"Admin {message.from_user.id} started blocking process")

async def handle_user_id_input(message: Message, state: FSMContext):
    try:
        user_id = int(message.text)
        admin_state_result = await safe_execute_sql(
            'SELECT state FROM users WHERE user_id = ?',
            (ADMIN_ID,),
            fetch_one=True
        )
        admin_state = admin_state_result[0]

        if admin_state == 'waiting_for_user_block':
            await handle_user_block(message, user_id)
        elif admin_state == 'waiting_for_user_unblock':
            await handle_user_unblock(message, user_id)
        elif admin_state == 'waiting_for_user_deletion':
            await handle_user_deletion(message, user_id)
    except ValueError:
        await message.answer("❌ Будь ласка, введіть правильний ID користувача (тільки цифри)")
        await show_user_management(message)

async def handle_user_block(message: Message, bot: Bot, user_id_to_block: int, state: FSMContext):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Перевіряємо, чи існує користувач
    user = await safe_execute_sql(
        'SELECT user_id, username FROM users WHERE user_id = ?',
        (user_id_to_block,),
        fetch_one=True
    )

    if not user:
        await message.answer(f"❌ Користувач з ID {user_id_to_block} не знайдений.")
        await state.clear()
        await show_user_management(message)
        return

    # Перевіряємо, чи користувач уже заблокований
    is_blocked = await safe_execute_sql(
        'SELECT is_blocked FROM users WHERE user_id = ?',
        (user_id_to_block,),
        fetch_one=True
    )
    if is_blocked and is_blocked[0] == 1:
        await message.answer(f"⚠️ Користувач з ID {user_id_to_block} уже заблокований.")
        await state.clear()
        await show_user_management(message)
        return

    # Блокуємо користувача
    await safe_execute_sql(
        'UPDATE users SET is_blocked = 1 WHERE user_id = ?',
        (user_id_to_block,)
    )

    # Повідомлення адміну
    await message.answer(f"✅ Користувач з ID {user_id_to_block} заблокований.")

    # Повідомлення користувачу
    try:
        await message.bot.send_message(
            user_id_to_block,
            "🚫 Ваш аккаунт заблокирован."
        )
    except Exception as e:
        await message.answer(f"❌ Помилка при відправці повідомлення користувачу: {str(e)}")

    # Очищаємо стан і повертаємо до меню
    await state.clear()
    await show_user_management(message)
    logging.info(f"Admin {user_id} blocked user {user_id_to_block}")

# Функції для розблокування користувачів
async def start_user_unblock(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return
    await message.answer("Введіть ID користувача, якого бажаєте розблокувати:")
    await state.set_state("waiting_for_user_unblock")  # Встановлюємо стан через FSMContext
    logging.info(f"Admin {message.from_user.id} started unblocking process")

async def handle_user_unblock(message: Message, bot: Bot, user_id_to_unblock: int, state: FSMContext):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Перевіряємо, чи існує користувач
    user = await safe_execute_sql(
        'SELECT user_id, username, is_blocked FROM users WHERE user_id = ?',
        (user_id_to_unblock,),
        fetch_one=True
    )

    if not user:
        await message.answer(f"❌ Користувач з ID {user_id_to_unblock} не знайдений.")
        await state.clear()
        await show_user_management(message)
        return

    # Перевіряємо, чи користувач заблокований
    if user[2] == 0:
        await message.answer(f"⚠️ Користувач з ID {user_id_to_unblock} не заблокований.")
        await state.clear()
        await show_user_management(message)
        return

    # Розблоковуємо користувача
    await safe_execute_sql(
        'UPDATE users SET is_blocked = 0 WHERE user_id = ?',
        (user_id_to_unblock,)
    )

    # Повідомлення адміну
    await message.answer(f"✅ Користувач з ID {user_id_to_unblock} розблокований.")

    # Повідомлення користувачу
    try:
        await message.bot.send_message(
            user_id_to_unblock,
            "✅ Ваш аккаунт разблокирован."
        )
    except Exception as e:
        await message.answer(f"❌ Помилка при відправці повідомлення користувачу: {str(e)}")

    # Очищаємо стан і повертаємо до меню
    await state.clear()
    await show_user_management(message)
    logging.info(f"Admin {user_id} unblocked user {user_id_to_unblock}")

# Оновлена функція для видалення користувача
async def start_user_deletion(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return
    await message.answer("Введіть ID користувача, якого бажаєте видалити:")
    await state.set_state("waiting_for_user_deletion")  # Встановлюємо стан через FSMContext
    logging.info(f"Admin {message.from_user.id} started deletion process")

async def handle_user_deletion(message: Message, bot: Bot, user_id_to_delete: int, state: FSMContext):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Перевіряємо, чи існує користувач
    user = await safe_execute_sql(
        'SELECT user_id, username FROM users WHERE user_id = ?',
        (user_id_to_delete,),
        fetch_one=True
    )

    if not user:
        await message.answer(f"❌ Користувач з ID {user_id_to_delete} не знайдений.")
        await state.clear()
        await show_user_management(message)
        return

    # Видаляємо користувача та пов’язані записи
    try:
        conn = sqlite3.connect('bot_database.db')
        c = conn.cursor()

        # Список таблиць і полів для видалення
        tables_and_fields = [
            ('transactions', 'user_id'),
            ('used_promo_codes', 'user_id'),
            ('temp_referrals', 'user_id'),
            ('referral_history', 'referrer_id'),
            ('referral_history', 'referral_user_id')
        ]

        for table, field in tables_and_fields:
            c.execute(f'DELETE FROM {table} WHERE {field} = ?', (user_id_to_delete,))

        # Видаляємо запис користувача
        c.execute('DELETE FROM users WHERE user_id = ?', (user_id_to_delete,))
        conn.commit()

        # Повідомлення адміну
        await message.answer(f"✅ Користувач з ID {user_id_to_delete} та всі пов'язані записи видалені.")
        logging.info(f"Admin {user_id} deleted user {user_id_to_delete} and all related records")
    except Exception as e:
        await message.answer(f"❌ Помилка при видаленні користувача: {str(e)}")
        logging.error(f"Error deleting user {user_id_to_delete}: {str(e)}")
    finally:
        conn.close()
        await state.clear()
        await show_user_management(message)


# Клавіатура для вибору суми обміну
def get_exchange_keyboard():
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="10⭐️", callback_data="exchange_10"),
         types.InlineKeyboardButton(text="50⭐️", callback_data="exchange_50")],
        [types.InlineKeyboardButton(text="100⭐️", callback_data="exchange_100"),
         types.InlineKeyboardButton(text="Другая сумма⭐️", callback_data="exchange_custom")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")]
    ])
    return keyboard

# Клавіатура для отмены (отдельная от инвойса)
def get_cancel_keyboard(payload):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{payload}")]
    ])

# Обробник команди "Обмен Stars⭐️"
@router.message(F.text == "Обмен Stars⭐️")
async def exchange_stars_command(message: Message, bot: Bot, state: FSMContext):
    await message.answer(
        "Выберите количество Telegram Stars⭐️ для обмена (100⭐️ = 3$):",
        reply_markup=get_exchange_keyboard()
    )
    await state.set_state(UserState.waiting_for_stars_amount)

# Обробник вибору фіксованої суми
@router.callback_query(F.data.startswith("exchange_"), UserState.waiting_for_stars_amount)
async def process_exchange_amount(callback: CallbackQuery, bot: Bot, state: FSMContext):
    amount = callback.data.split("_")[1]

    if amount == "custom":
        await callback.message.edit_text(
            "Введите количество Stars⭐️ для обмена (мин. 1⭐️):"
        )
        await state.set_state(UserState.waiting_for_amount)
    else:
        stars = int(amount)
        dollars = stars * 3 / 100  # Конвертация: 100⭐️ = 3$
        payload = f"exchange_{stars}_{callback.from_user.id}"

        logging.info(f"Создан инвойс для {callback.from_user.id}: {stars}⭐️ -> {dollars}$")
        # Отправляем инвойс без reply_markup
        invoice_msg = await bot.send_invoice(
            chat_id=callback.from_user.id,
            title=f"Обмен {stars}⭐️",
            description=f"Обменяйте {stars} Telegram Stars на {dollars}$",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{stars} Stars", amount=stars)]
        )
        # Отправляем отдельное сообщение с кнопкой "Отменить"
        await bot.send_message(
            chat_id=callback.from_user.id,
            text="Нажмите ниже, чтобы отменить обмен:",
            reply_markup=get_cancel_keyboard(payload)
        )
        await state.update_data(invoice_msg_id=invoice_msg.message_id)  # Сохраняем ID инвойса для удаления
        await callback.message.delete()

# Обробник відміни платежу
@router.callback_query(F.data.startswith("cancel_"))
async def process_cancel_payment(callback: CallbackQuery, bot: Bot, state: FSMContext):
    data = await state.get_data()
    invoice_msg_id = data.get("invoice_msg_id")

    # Удаляем инвойс, если он еще существует
    if invoice_msg_id:
        try:
            await bot.delete_message(chat_id=callback.from_user.id, message_id=invoice_msg_id)
        except TelegramAPIError as e:
            logging.warning(f"Не удалось удалить инвойс {invoice_msg_id}: {str(e)}")

    await callback.message.delete()  # Удаляем сообщение с кнопкой "Отменить"
    await callback.message.answer("❌ Обмен отменен.")
    await state.clear()
    logging.info(f"Пользователь {callback.from_user.id} отменил обмен")

# Обробник передоплати (перевірка)
@router.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery, bot: Bot):
    await bot.answer_pre_checkout_query(
        pre_checkout_query.id,
        ok=True
    )

# Обробник успішної оплати
@router.message(F.successful_payment)
async def process_successful_payment(message: Message, bot: Bot, state: FSMContext):
    payment = message.successful_payment
    payload = payment.invoice_payload.split("_")
    stars = int(payload[1])
    user_id = int(payload[2])
    dollars = stars * 3 / 100  # Конвертація

    logging.info(f"Успешная оплата для {user_id}: {stars}⭐️ -> {dollars}$ (payload: {payment.invoice_payload})")

    conn = sqlite3.connect('bot_database.db')
    try:
        c = conn.cursor()
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        current_balance = c.fetchone()
        if current_balance:
            old_balance = current_balance[0]
            new_balance = old_balance + dollars
            logging.info(f"Баланс до: {old_balance}$, после: {new_balance}$")
        else:
            old_balance = 0
            new_balance = dollars
            logging.warning(f"Пользователь {user_id} не найден, создаем с балансом {dollars}$")

        c.execute("SELECT id FROM transactions WHERE user_id = ? AND amount = ? AND type = 'exchange' AND status = 'completed' ORDER BY created_at DESC LIMIT 1",
                 (user_id, dollars))
        existing_tx = c.fetchone()
        if existing_tx:
            logging.error(f"Дублирование транзакции для {user_id}: {stars}⭐️ -> {dollars}$ уже обработано (ID: {existing_tx[0]})")
            await message.answer("❌ Этот платеж уже был обработан ранее!")
            return

        c.execute(
            "UPDATE users SET balance = ? WHERE user_id = ?",
            (new_balance, user_id)
        )
        c.execute(
            "INSERT INTO transactions (user_id, amount, type, status) VALUES (?, ?, ?, ?)",
            (user_id, dollars, "exchange", "completed")
        )
        conn.commit()

        await message.answer(
            f"✅ Успешно обменяно {stars}⭐️ на {dollars}$!\nВаш баланс обновлен.\nНовый баланс: {new_balance}$"
        )
    except sqlite3.Error as e:
        logging.error(f"Ошибка базы данных при обмене для {user_id}: {str(e)}")
        await message.answer(f"❌ Ошибка при обмене: {str(e)}")
        await notify_admin(f"Ошибка обмена для {user_id}: {str(e)}")
    finally:
        conn.close()

    await state.clear()

# Обробник введення власної суми
async def process_custom_amount(message: Message, bot: Bot, state: FSMContext):
    try:
        stars = int(message.text)
        if stars < 1:
            await message.answer("❌ Количество Stars⭐️ должно быть больше 0!")
            return

        dollars = stars * 3 / 100
        payload = f"exchange_{stars}_{message.from_user.id}"
        logging.info(f"Создан инвойс для {message.from_user.id}: {stars}⭐️ -> {dollars}$ (ручной ввод)")
        invoice_msg = await bot.send_invoice(
            chat_id=message.from_user.id,
            title=f"Обмен {stars}⭐️",
            description=f"Обменяйте {stars} Telegram Stars на {dollars}$",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{stars} Stars", amount=stars)]
        )
        await bot.send_message(
            chat_id=message.from_user.id,
            text="Нажмите ниже, чтобы отменить обмен:",
            reply_markup=get_cancel_keyboard(payload)
        )
        await state.update_data(invoice_msg_id=invoice_msg.message_id)
        # Не очищаємо стан тут, щоб invoice_msg_id був доступний для скасування
    except ValueError:
        await message.answer("❌ Введите правильное число Stars⭐️!")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        await notify_admin(f"Ошибка для {message.from_user.id}: {str(e)}")
        await state.clear()  # Очищаємо тільки при критичній помилці

# Управління каналами через основний router
@router.message(F.text == "📁 Управління каналами")
async def show_channel_management(message: Message, bot: Bot, state: FSMContext = None):
    """Відображає панель управління каналами"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.answer("У вас немає доступу до цього розділу.")
        return

    # Отримуємо список всіх каналів
    channels = await safe_execute_sql(
        'SELECT channel_id, channel_name, channel_link, is_required FROM channels'
    )

    # Створюємо список кнопок для inline_keyboard
    inline_keyboard = [
        [InlineKeyboardButton(text="➕ Додати новий канал", callback_data="add_new_channel")]
    ]

    if channels:
        for channel in channels:
            channel_id, channel_name, channel_link, is_required = channel
            status = "🔒 Обов'язковий" if is_required else "🔓 Необов'язковий"
            inline_keyboard.append(
                [InlineKeyboardButton(text=f"{channel_name} - {status}", callback_data=f"channel_info_{channel_id}")]
            )

    # Ініціалізуємо InlineKeyboardMarkup із inline_keyboard
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard, row_width=1)

    await message.answer("📁 Управління каналами\n\nОберіть дію або канал для управління:", reply_markup=markup)

@router.callback_query(F.data == "add_new_channel")
async def start_adding_channel(call: CallbackQuery, bot: Bot, state: FSMContext):
    """Початок процесу додавання нового каналу"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.")
        return

    await call.answer()
    await call.message.answer("📢 Введіть посилання на канал або тег (наприклад, @назватегу або https://t.me/назватегу):")
    await state.set_state(UserState.waiting_for_channel_add)
    current_state = await state.get_state()
    logging.info(f"Користувач {user_id} увійшов у стан очікування додавання каналу. Поточний стан: {current_state}")

@router.message(UserState.waiting_for_channel_add)
async def process_new_channel(message: Message, bot: Bot, state: FSMContext):
    """Обробка введеного каналу користувачем"""
    user_id = message.from_user.id
    channel_input = message.text.strip()

    logging.info(f"Користувач {user_id} у стані waiting_for_channel_add ввів: {channel_input}")

    try:
        # Розширена обробка формату введення
        if channel_input.startswith('@'):
            channel_tag = channel_input
        elif 't.me/' in channel_input:
            channel_username = channel_input.split('t.me/')[-1].split('/')[0].split('+')[0]
            channel_tag = '@' + channel_username if channel_username else channel_input
        elif 'fragment.com/' in channel_input or 'telegram.me/' in channel_input:
            channel_username = channel_input.split('/')[-1].replace('channel', '').strip()
            channel_tag = '@' + channel_username if channel_username else channel_input
        else:
            if channel_input.strip().isalnum():
                channel_tag = '@' + channel_input
            else:
                await message.answer("❌ Невірний формат. Введіть тег каналу (наприклад, @channel) або посилання (https://t.me/channel).")
                logging.info(f"Користувач {user_id} ввів некоректний формат: {channel_input}")
                await state.clear()
                return

        logging.info(f"Оброблено тег каналу: {channel_tag}")
        await message.answer("⏳ Перевіряю канал...")
        await check_channel(message, channel_tag, state)

    except Exception as e:
        logging.error(f"Помилка в process_new_channel для {user_id}: {str(e)}")
        await message.answer(f"❌ Помилка: {str(e)}")
        await state.clear()

async def check_channel(message: Message, bot: Bot, channel_tag: str, state: FSMContext):
    """Перевірка та додавання каналу"""
    user_id = message.from_user.id
    try:
        # Спробуємо отримати інформацію про канал
        chat = await bot.get_chat(channel_tag)

        # Перевіряємо права бота
        bot_info = await bot.get_me()
        bot_member = await bot.get_chat_member(chat.id, bot_info.id)

        if bot_member.status not in ['administrator', 'creator']:
            await message.answer(
                "❌ Бот повинен бути адміністратором каналу!\n\n"
                "1. Додайте бота як адміністратора каналу\n"
                "2. Надайте боту права на читання повідомлень\n"
                "3. Спробуйте додати канал знову"
            )
            await state.clear()
            return

        # Отримуємо дані про канал
        channel_id = str(chat.id)
        channel_name = chat.title

        # Формуємо посилання на канал
        if chat.username:
            channel_link = f"https://t.me/{chat.username}"
        elif hasattr(chat, 'invite_link') and chat.invite_link:
            channel_link = chat.invite_link
        else:
            try:
                invite_link = await bot.create_chat_invite_link(chat.id)
                channel_link = invite_link.invite_link
            except Exception:
                channel_link = f"https://t.me/c/{str(channel_id)[4:]}" if channel_id.startswith('-100') else f"https://t.me/c/{channel_id}"

        # Перевіряємо, чи канал вже існує в базі
        existing_channel = await safe_execute_sql(
            'SELECT channel_id FROM channels WHERE channel_id = ?',
            (channel_id,),
            fetch_one=True
        )

        if existing_channel:
            await message.answer("❌ Цей канал вже додано до бази даних.")
            await state.clear()
            return

        # Додаємо канал до бази даних
        await safe_execute_sql(
            'INSERT INTO channels (channel_id, channel_name, channel_link, is_required) VALUES (?, ?, ?, ?)',
            (channel_id, channel_name, channel_link, 1)
        )

        # Створюємо клавіатуру для перевірки каналу
         # Створюємо клавіатуру з явним визначенням типу
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📢 Перейти до каналу",
                    url=channel_link
                )
            ],
            [
                InlineKeyboardButton(
                    text="📁 Управління каналами",
                    callback_data="manage_channels"
                )
            ]
        ])

        await message.answer(
            f"✅ Канал успішно додано!\n\n"
            f"📌 Назва: {channel_name}\n"
            f"🔗 Посилання: {channel_link}\n"
            f"🆔 ID: {channel_id}\n\n"
            f"Зараз канал встановлено як обов'язковий для підписки.",
            reply_markup=markup
        )
        logging.info(f"Канал {channel_name} (ID: {channel_id}) успішно додано для {user_id}")

        # Надсилаємо звіт адміну (якщо операцію виконував не адмін)
        if user_id != ADMIN_ID:
            await bot.send_message(ADMIN_ID,
                f"ℹ️ Додано новий канал\n\n"
                f"👤 Адміністратор: {message.from_user.first_name} (ID: {user_id})\n"
                f"📌 Канал: {channel_name}\n"
                f"🆔 ID каналу: {channel_id}"
            )

    except TelegramAPIError as api_error:
        error_message = str(api_error)
        if "chat not found" in error_message.lower():
            await message.answer("❌ Канал не знайдено. Перевірте правильність посилання або тегу.")
        elif "bot is not a member" in error_message.lower():
            await message.answer("❌ Бот не є учасником каналу. Додайте бота до каналу спочатку.")
        else:
            await message.answer(
                f"❌ Помилка при додаванні каналу:\n\n{error_message}\n\n"
                f"Переконайтеся, що:\n1. Бот доданий до каналу\n2. Бот має права адміністратора\n3. Посилання/тег каналу правильні"
            )
            if user_id == ADMIN_ID:
                await bot.send_message(ADMIN_ID, f"🔍 Технічна помилка API: {error_message}")
    except Exception as e:
        error_message = str(e)
        await message.answer(
            f"❌ Помилка при додаванні каналу:\n\n{error_message}\n\n"
            f"Переконайтеся, що:\n1. Бот доданий до каналу\n2. Бот має права адміністратора\n3. Посилання/тег каналу правильні"
        )
        if user_id == ADMIN_ID:
            await bot.send_message(ADMIN_ID, f"🔍 Технічна помилка: {error_message}")
    finally:
        await state.clear()

@router.callback_query(F.data.startswith("channel_info_"))
async def show_channel_info(call: CallbackQuery, bot: Bot, state: FSMContext):
    """Відображає інформацію про канал та варіанти дій з ним"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.")
        return

    channel_id = call.data.split("_")[2]
    channel_data = await safe_execute_sql(
        'SELECT channel_name, channel_link, is_required FROM channels WHERE channel_id = ?',
        (channel_id,),
        fetch_one=True
    )

    if not channel_data:
        await call.answer("❌ Канал не знайдено в базі даних.")
        return

    channel_name, channel_link, is_required = channel_data

    # Формуємо список кнопок для inline_keyboard
    inline_keyboard = [
        [InlineKeyboardButton(text="🔗 Перейти до каналу", url=channel_link)],
        [InlineKeyboardButton(
            text="🔓 Зробити необов'язковим" if is_required else "🔒 Зробити обов'язковим",
            callback_data=f"toggle_required_{channel_id}"
        )],
        [InlineKeyboardButton(text="❌ Видалити канал", callback_data=f"delete_channel_{channel_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="manage_channels")]
    ]

    # Створюємо клавіатуру з передачею inline_keyboard
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard, row_width=1)

    status = "🔒 Обов'язковий" if is_required else "🔓 Необов'язковий"
    await call.message.edit_text(
        f"📢 Інформація про канал\n\n"
        f"📌 Назва: {channel_name}\n"
        f"🆔 ID: {channel_id}\n"
        f"🔗 Посилання: {channel_link}\n"
        f"📊 Статус: {status}\n\n"
        f"Оберіть дію:",
        reply_markup=markup
    )
    await call.answer()

@router.callback_query(F.data == "manage_channels")
async def manage_channels_callback(call: CallbackQuery, bot: Bot):
    """Обробник повернення до управління каналами"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.", show_alert=True)
        return

    await call.answer()
    # Отримуємо список всіх каналів
    channels = await safe_execute_sql(
        'SELECT channel_id, channel_name, channel_link, is_required FROM channels'
    )

    # Створюємо список кнопок для inline_keyboard
    inline_keyboard = [
        [InlineKeyboardButton(text="➕ Додати новий канал", callback_data="add_new_channel")]
    ]

    if channels:
        for channel in channels:
            channel_id, channel_name, channel_link, is_required = channel
            status = "🔒 Обов'язковий" if is_required else "🔓 Необов'язковий"
            inline_keyboard.append(
                [InlineKeyboardButton(text=f"{channel_name} - {status}", callback_data=f"channel_info_{channel_id}")]
            )

    # Ініціалізуємо InlineKeyboardMarkup із inline_keyboard
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard, row_width=1)

    await call.message.edit_text(
        "📁 Управління каналами\n\nОберіть дію або канал для управління:",
        reply_markup=markup
    )

@router.callback_query(F.data.startswith("toggle_required_"))
async def toggle_channel_required(call: CallbackQuery, bot: Bot):
    """Змінює статус обов'язковості каналу"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.")
        return

    channel_id = call.data.split("_")[2]
    current_status = await safe_execute_sql(
        'SELECT is_required FROM channels WHERE channel_id = ?',
        (channel_id,),
        fetch_one=True
    )

    if not current_status:
        await call.answer("❌ Канал не знайдено в базі даних.")
        return

    new_status = 0 if current_status[0] == 1 else 1
    await safe_execute_sql(
        'UPDATE channels SET is_required = ? WHERE channel_id = ?',
        (new_status, channel_id)
    )

    status_text = "обов'язковим" if new_status == 1 else "необов'язковим"
    await call.answer(f"✅ Канал зроблено {status_text}")
    channel_data = await safe_execute_sql(
        'SELECT channel_name, channel_link, is_required FROM channels WHERE channel_id = ?',
        (channel_id,),
        fetch_one=True
    )
    channel_name, channel_link, is_required = channel_data
    markup = InlineKeyboardMarkup(row_width=1)
    status_text = "🔓 Зробити необов'язковим" if is_required else "🔒 Зробити обов'язковим"
    markup.add(
        InlineKeyboardButton(text="🔗 Перейти до каналу", url=channel_link),
        InlineKeyboardButton(text=status_text, callback_data=f"toggle_required_{channel_id}"),
        InlineKeyboardButton(text="❌ Видалити канал", callback_data=f"delete_channel_{channel_id}"),
        InlineKeyboardButton(text="◀️ Назад", callback_data="manage_channels")
    )
    status = "🔒 Обов'язковий" if is_required else "🔓 Необов'язковий"
    await call.message.edit_text(
        f"📢 Інформація про канал\n\n"
        f"📌 Назва: {channel_name}\n"
        f"🆔 ID: {channel_id}\n"
        f"🔗 Посилання: {channel_link}\n"
        f"📊 Статус: {status}\n\n"
        f"Оберіть дію:",
        reply_markup=markup
    )

@router.callback_query(F.data.startswith("delete_channel_"))
async def confirm_delete_channel(call: CallbackQuery, bot: Bot):
    """Запитує підтвердження видалення каналу"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.")
        return

    channel_id = call.data.split("_")[2]
    channel_data = await safe_execute_sql(
        'SELECT channel_name FROM channels WHERE channel_id = ?',
        (channel_id,),
        fetch_one=True
    )

    if not channel_data:
        await call.answer("❌ Канал не знайдено в базі даних.")
        return

    channel_name = channel_data[0]

    # Формуємо список кнопок для inline_keyboard
    inline_keyboard = [
        [
            InlineKeyboardButton(text="✅ Так, видалити", callback_data=f"confirm_delete_{channel_id}"),
            InlineKeyboardButton(text="❌ Ні, скасувати", callback_data=f"channel_info_{channel_id}")
        ]
    ]

    # Створюємо клавіатуру з передачею inline_keyboard
    markup = InlineKeyboardMarkup(inline_keyboard=inline_keyboard, row_width=2)

    await call.message.edit_text(
        f"⚠️ Підтвердження видалення\n\n"
        f"Ви впевнені, що хочете видалити канал «{channel_name}»?\n\n"
        f"Після видалення канал більше не буде перевірятися при підписці користувачів.",
        reply_markup=markup
    )
    await call.answer()

@router.callback_query(F.data.startswith("confirm_delete_"))
async def delete_channel(call: CallbackQuery, bot: Bot):
    """Видаляє канал з бази даних"""
    user_id = call.from_user.id
    if user_id != ADMIN_ID:
        await call.answer("У вас немає доступу до цього розділу.")
        return

    channel_id = call.data.split("_")[2]
    channel_data = await safe_execute_sql(
        'SELECT channel_name FROM channels WHERE channel_id = ?',
        (channel_id,),
        fetch_one=True
    )

    if not channel_data:
        await call.answer("❌ Канал не знайдено в базі даних.")
        return

    channel_name = channel_data[0]
    await safe_execute_sql('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
    await call.answer("✅ Канал успішно видалено")
    await call.message.edit_text(f"✅ Канал «{channel_name}» успішно видалено.")
    await asyncio.sleep(1)
    await show_channel_management(call.message)

@router.message(F.text == "⚙️ Настройки")
async def show_settings_menu(message: Message, bot: Bot, state: FSMContext = None):
    await _show_settings_menu(message, is_callback=False)

async def _show_settings_menu(message, bot: Bot, is_callback=False):
    try:
        user_id = message.from_user.id

        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        cursor.execute('SELECT hide_in_leaderboard FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()

        if result is None:
            hide_in_leaderboard = 0
        else:
            hide_in_leaderboard = result[0]

        logging.info(f"Read hide_in_leaderboard for user_id={user_id}: {hide_in_leaderboard}")

        hide_text = "🙈 Скрыть имя в таблице лидеров" if not hide_in_leaderboard else "🙉 Показать имя в таблице лидеров"

        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=hide_text, callback_data="toggle_hide_leaderboard")],
            [InlineKeyboardButton(text="↩️ Назад", callback_data="back_to_main")]
        ])

        # Додаємо видимий статус і невидимий унікальний суфікс
        status_text = "скрыто" if hide_in_leaderboard else "показано"
        unique_suffix = chr(8203) * (int(time.time() * 1000) % 10 + 1)
        text = f"⚙️ Настройки:\nСтатус: {status_text}{unique_suffix}"

        if is_callback:
            logging.info(f"Спроба редагувати повідомлення для user_id={user_id} з текстом: '{text}' і кнопкою: '{hide_text}'")
            await message.edit_text(text, reply_markup=markup)
        else:
            logging.info(f"Відправка нового повідомлення для user_id={user_id} з текстом: '{text}'")
            await message.answer(text, reply_markup=markup)

    except TelegramAPIError as e:
        logging.error(f"Telegram API помилка при редагуванні меню налаштувань: {str(e)}")
        if "message is not modified" in str(e):
            logging.info("Повідомлення не змінилося, ігноруємо помилку (але це не має статися)")
        else:
            if is_callback:
                await message.edit_text("❌ Помилка редагування налаштувань. Спробуйте ще раз.")
            else:
                await message.answer("❌ Ошибка загрузки настроек. Попробуйте позже.")
    except Exception as e:
        logging.error(f"Інша помилка при показі меню налаштувань: {str(e)}")
        if is_callback:
            await message.edit_text("❌ Ошибка загрузки настроек. Попробуйте позже.")
        else:
            await message.answer("❌ Ошибка загрузки настроек. Попробуйте позже.")

@router.callback_query(F.data == "toggle_hide_leaderboard")
async def toggle_hide_leaderboard(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id

    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        cursor.execute('SELECT hide_in_leaderboard FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()

        if result is None:
            await call.answer("❌ Ошибка доступа к данным пользователя", show_alert=True)
            conn.close()
            return

        current_hide_status = result[0]
        new_hide_status = 0 if current_hide_status else 1

        cursor.execute('UPDATE users SET hide_in_leaderboard = ? WHERE user_id = ?',
                      (new_hide_status, user_id))
        conn.commit()
        logging.info(f"User {user_id} hide_in_leaderboard updated to {new_hide_status}")
        conn.close()

        status_text = "скрыто" if new_hide_status else "отображается"
        await call.answer(f"✅ Ваше имя теперь {status_text} в таблице лидеров", show_alert=True)

        await _show_settings_menu(call.message, is_callback=True)

    except Exception as e:
        logging.error(f"Ошибка при изменении видимости: {str(e)}")
        await call.answer("❌ Ошибка обновления настроек. Попробуйте позже.", show_alert=True)

async def start_user_info(message: Message, bot: Bot, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("У вас нет доступа к этому разделу.")
        return
    await message.answer("Введите ID пользователя, информацию о котором хотите получить:")
    await state.set_state("waiting_for_user_info")
    logging.info(f"Admin {message.from_user.id} запросил информацию о пользователе")

async def show_user_info(message: Message, bot: Bot, target_user_id: int, state: FSMContext):
    try:
        # Отримуємо базову інформацію про користувача
        user_data = await safe_execute_sql(
            "SELECT username, balance, total_earnings, join_date, is_blocked FROM users WHERE user_id = ?",
            (target_user_id,),
            fetch_one=True
        )

        if not user_data:
            await message.answer(f"❌ Пользователь с ID {target_user_id} не найден!")
            await state.clear()
            await show_admin_panel(message)
            return

        username, balance, total_earnings, join_date, is_blocked = user_data
        username_display = f"@{username}" if username else "Без ника"
        blocked_status = "Заблокирован" if is_blocked else "Активен"

        # Отримуємо кількість рефералів
        referral_count = await safe_execute_sql(
            "SELECT COUNT(*) FROM referral_history WHERE referrer_id = ?",
            (target_user_id,),
            fetch_one=True
        )
        referral_count = referral_count[0] if referral_count else 0

        # Формуємо повідомлення з інформацією
        response = (
            f"ℹ️ Информация о пользователе:\n\n"
            f"🆔 ID: {target_user_id}\n"
            f"👤 Ник: {username_display}\n"
            f"💰 Баланс: {balance:.2f}$\n"
            f"📈 Общий заработок: {total_earnings:.2f}$\n"
            f"👥 Количество рефералов: {referral_count}\n"
            f"📅 Дата присоединения: {join_date}\n"
            f"🚫 Статус: {blocked_status}"
        )

        # Додаємо кнопку "Назад" до адмін-панелі
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ Назад к админ-панели", callback_data="back_to_admin")]
        ])

        await message.answer(response, reply_markup=keyboard)
        logging.info(f"Показана информация для пользователя {target_user_id} админу {message.from_user.id}")

    except Exception as e:
        logging.error(f"Ошибка при получении информации для {target_user_id}: {str(e)}")
        await message.answer("❌ Ошибка при получении информации. Попробуйте позже.")
        await notify_admin(f"Ошибка в show_user_info для {target_user_id}: {str(e)}")

    await state.clear()
    await show_admin_panel(message)

def check_table_structure():
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        # Отримуємо інформацію про структуру таблиці channels
        cursor.execute("PRAGMA table_info(channels)")
        columns = cursor.fetchall()
        print("Структура таблиці channels:", columns)

        conn.close()
    except Exception as e:
        print(f"Помилка при перевірці структури таблиці: {str(e)}")

async def start_polling_with_retry(dp: Dispatcher, bot: Bot, max_retries=5, delay=5):
    """
    Запускає polling із повторними спробами при помилках.
    """
    retries = 0
    while retries < max_retries:
        try:
            await dp.start_polling(bot, skip_updates=True)
            break  # Якщо polling успішно завершився, виходимо з циклу
        except Exception as e:
            logging.error(f"Помилка polling: {e}. Спроба {retries + 1} із {max_retries}...")
            retries += 1
            if retries < max_retries:
                await asyncio.sleep(delay)
            else:
                logging.error(f"Не вдалося запустити polling після {max_retries} спроб.")
                await notify_admin(bot, f"Не вдалося запустити polling після {max_retries} спроб.")
                raise

async def run_bot_once(bot: Bot):
    ensure_database_exists()
    update_db_for_user_blocking()
    create_promo_codes_table()
    update_db_for_settings()

    await safe_execute_sql('''INSERT OR IGNORE INTO channels (channel_id, channel_name, channel_link, is_required)
                            VALUES (?, ?, ?, ?)''',
                          ('-1002157115077', 'CryptoWave', 'https://t.me/CryptoWaveee', 1))
    logging.info("Тестовий канал додано або вже існує")

    dp = Dispatcher()
    dp.include_router(router)

    try:
        logging.info("Перевірка типу bot: %s", type(bot))
        logging.info("Перевірка методів bot: get_me=%s, send_message=%s", bot.get_me, bot.send_message)
        logging.info("Попытка получить информацию о боте...")
        bot_info = await bot.get_me()
        logging.info(f"Бот успешно подключен: {bot_info.username}")
    except Exception as e:
        logging.error(f"Ошибка подключения к Telegram: {e}", exc_info=True)
        await notify_admin(bot, f"Ошибка подключения к Telegram: {str(e)}")
        return

    try:
        await start_polling_with_retry(dp, bot, max_retries=5, delay=5)
    except Exception as e:
        logging.error(f"❌ Помилка запуску бота: {e}")
        await notify_admin(bot, f"❌ Помилка запуску бота: {str(e)}")
    finally:
        await bot.session.close()
        logging.info("🔄 Сесія бота закрита.")

async def main():
    # Ініціалізація бота без проксі (Replit має прямий доступ до мережі)
    bot = Bot(token=API_TOKEN)
    try:
        await run_bot_once(bot)
    except Exception as e:
        logging.error(f"Помилка в main: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
