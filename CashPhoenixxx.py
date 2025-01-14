import telebot
from telebot import types
import sqlite3
import os
from telebot.handler_backends import State, StatesGroup
from telebot.apihelper import ApiException

# Конфігураційні константи
bot = telebot.TeleBot(os.getenv('BOT_TOKEN'))
ADMIN_ID = 1270564746
CHANNEL_ID = '@CryptoWaveee'
REFERRAL_REWARD = 0.5
MIN_WITHDRAWAL = 10.0

# Налаштування шляхів для бази даних
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_DIR = os.path.join(BASE_DIR, 'database')
DATABASE_PATH = os.path.join(DATABASE_DIR, 'bot_database.db')

def ensure_database_exists():
    """Функція для перевірки та створення необхідних директорій та бази даних"""
    try:
        # Створюємо директорію для бази даних, якщо вона не існує
        if not os.path.exists(DATABASE_DIR):
            os.makedirs(DATABASE_DIR)
            print(f"✅ Створено директорію бази даних: {DATABASE_DIR}")

        # Перевіряємо чи існує файл бази даних
        if not os.path.exists(DATABASE_PATH):
            # Створюємо з'єднання, що автоматично створить файл бази даних
            conn = sqlite3.connect(DATABASE_PATH)
            conn.close()
            print(f"✅ Створено файл бази даних: {DATABASE_PATH}")
            return True
        return True
    except Exception as e:
        print(f"❌ Помилка при створенні бази даних: {str(e)}")
        return False


# Клас для станів користувача
class UserState:
    none = 'NONE'
    waiting_for_withdrawal = 'WAITING_FOR_WITHDRAWAL'
    waiting_for_broadcast = 'WAITING_FOR_BROADCAST'
    waiting_for_channel_add = 'WAITING_FOR_CHANNEL_ADD'
    waiting_for_balance_change = 'WAITING_FOR_BALANCE_CHANGE'
    waiting_promo = 'waiting_promo'
    waiting_admin_promo = 'waiting_admin_promo'
    waiting_admin_reward = 'waiting_admin_reward'
    waiting_admin_activations = 'waiting_admin_activations'


def safe_db_connect():
    """Функція для безпечного підключення до бази даних"""
    try:
        # Спочатку переконуємося, що база даних існує
        if not ensure_database_exists():
            raise Exception("Не вдалося забезпечити існування бази даних")
            
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        return conn
    except sqlite3.Error as e:
        error_message = f"❌ Помилка підключення до БД: {str(e)}"
        print(error_message)
        bot.send_message(ADMIN_ID, error_message)
        return None

def safe_execute_sql(query, params=None, fetch_one=False):
    """Функція для безпечного виконання SQL-запитів"""
    try:
        # Перевіряємо наявність бази даних перед кожним запитом
        if not ensure_database_exists():
            raise Exception("База даних не доступна")

        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        print(f"Executing query: {query}")
        print(f"Parameters: {params}")
        
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        if fetch_one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
            
        print(f"Query result: {result}")
        
        conn.commit()
        conn.close()
        return result
    except Exception as e:
        error_message = f"Database error: {str(e)}"
        print(error_message)
        return None

def init_db():
    """Функція для ініціалізації бази даних"""
    if not ensure_database_exists():
        return

    conn = safe_db_connect()
    if not conn:
        return

    try:
        c = conn.cursor()

        # Таблиця користувачів
        c.execute('''CREATE TABLE IF NOT EXISTS users
            (user_id INTEGER PRIMARY KEY,
             username TEXT,
             balance REAL DEFAULT 0,
             total_earnings REAL DEFAULT 0,
             referrer_id INTEGER,
             join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
             last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
             state TEXT DEFAULT 'none',
             temp_data TEXT,
             FOREIGN KEY (referrer_id) REFERENCES users(user_id))''')

        # Таблиця транзакцій
        c.execute('''CREATE TABLE IF NOT EXISTS transactions
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
             user_id INTEGER,
             amount REAL,
             type TEXT,
             status TEXT,
             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
             FOREIGN KEY (user_id) REFERENCES users(user_id))''')

        # Таблиця каналів
        c.execute('''CREATE TABLE IF NOT EXISTS channels
            (channel_id TEXT PRIMARY KEY,
             channel_name TEXT,
             channel_link TEXT,
             added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
             is_required INTEGER DEFAULT 1)''')

        # Таблиця промокодів
        c.execute('''CREATE TABLE IF NOT EXISTS promo_codes
            (code TEXT PRIMARY KEY,
             reward REAL,
             max_activations INTEGER,
             current_activations INTEGER DEFAULT 0,
             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

        # Таблиця використаних промокодів
        c.execute('''CREATE TABLE IF NOT EXISTS used_promo_codes
            (user_id INTEGER,
             promo_code TEXT,
             activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
             PRIMARY KEY (user_id, promo_code),
             FOREIGN KEY (user_id) REFERENCES users (user_id),
             FOREIGN KEY (promo_code) REFERENCES promo_codes (code))''')

        # Таблиця тимчасових реферальних кодів
        c.execute('''CREATE TABLE IF NOT EXISTS temp_referrals
            (user_id INTEGER PRIMARY KEY,
             referral_code TEXT,
             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

        #Таблиця відстеження усіх рефералів
        c.execute('''CREATE TABLE IF NOT EXISTS referral_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            referrer_id INTEGER NOT NULL,
            referral_user_id INTEGER NOT NULL,
            reward_amount REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')


        conn.commit()
        print("✅ База даних успішно ініціалізована")
        print(f"📁 Шлях до бази даних: {DATABASE_PATH}")
    except Exception as e:
        print(f"❌ Помилка ініціалізації БД: {str(e)}")
        bot.send_message(ADMIN_ID, f"❌ Помилка ініціалізації БД: {str(e)}")
    finally:
        conn.close()

# Функція для створення таблиць промокодів
def create_promo_codes_table():
    safe_execute_sql('''
        CREATE TABLE IF NOT EXISTS promo_codes (
            code TEXT PRIMARY KEY,
            reward REAL,
            max_activations INTEGER,
            current_activations INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

def add_required_channel(channel_id, channel_name, channel_link):
    try:
        conn = safe_db_connect()
        if not conn:
            return False

        with conn:
            c = conn.cursor()
            c.execute('''
                INSERT INTO channels (channel_id, channel_name, channel_link, is_required)
                VALUES (?, ?, ?, 1)
            ''', (channel_id, channel_name, channel_link))
        return True
    except sqlite3.IntegrityError:
        bot.send_message(ADMIN_ID, "❌ Канал з таким ID вже існує в базі даних.")
        return False
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка додавання каналу: {str(e)}")
        return False


def check_subscription(user_id):
    try:
        channels = safe_execute_sql('SELECT channel_id FROM channels WHERE is_required = 1')
        print("Знайдені канали:", channels)

        if not channels:
            print("Немає каналів для перевірки")
            return True

        for channel in channels:
            try:
                member = bot.get_chat_member(channel[0], user_id)
                if member.status not in ['member', 'administrator', 'creator']:
                    return False
            except ApiException as e:
                print(f"Помилка перевірки каналу {channel[0]}: {str(e)}")
                continue
        return True
    except Exception as e:
        print(f"Загальна помилка: {str(e)}")
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

    # Добавте цей запит для перевірки
    user_exists = safe_execute_sql(
        'SELECT 1 FROM users WHERE user_id = ?',
        (user_id,),
        fetch_one=True
    )

    if not user_exists:
        print(f"User {user_id} not found in database")


def create_main_keyboard(user_id):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        types.KeyboardButton('💰 Баланс'),
        types.KeyboardButton('👥 Реферальная система'),
        types.KeyboardButton('💳 Вывести деньги'),
        types.KeyboardButton('📊 Моя статистика'),
        types.KeyboardButton('🍀 Промокод'),
        types.KeyboardButton('🏆 Таблица лидеров'),
        types.KeyboardButton('🛠️Тех.Поддержка')
    ]
    keyboard.add(*buttons)

    if user_id == ADMIN_ID:
        keyboard.add(types.KeyboardButton('🔑 Адмін панель'))

    return keyboard



# Виправлена функція start
@bot.message_handler(commands=['start'])
def start(message):
    if message.from_user.is_bot:
        return

    user_id = message.from_user.id
    username = message.from_user.username or "Anonymous"

    # Додаємо цей блок коду для збереження реферального коду
    if len(message.text.split()) > 1:
        referral_code = message.text.split()[1]
        safe_execute_sql(
            "INSERT OR REPLACE INTO temp_referrals (user_id, referral_code) VALUES (?, ?)",
            (user_id, referral_code)
        )

    try:
        # Перевірка підписки
        if not check_subscription(user_id):
            keyboard = types.InlineKeyboardMarkup()
            channels = safe_execute_sql(
                'SELECT channel_id, channel_name, channel_link FROM channels WHERE is_required = 1')

            if channels:
                for channel in channels:
                    keyboard.add(types.InlineKeyboardButton(
                        text=f"📢 {channel[1]}",
                        url=channel[2]
                    ))

                keyboard.add(types.InlineKeyboardButton(
                    text="✅ Проверить подписку",
                    callback_data="check_subscription"
                ))

                bot.send_message(user_id, "🔔 Для использования бота подпишитесь на наши каналы:",
                               reply_markup=keyboard)
                return

        # Перевіряємо чи користувач вже існує
        existing_user = safe_execute_sql(
            "SELECT user_id, referrer_id FROM users WHERE user_id = ?", 
            (user_id,), 
            fetch_one=True
        )
        
        # Обробка реферальної системи
        referral_code = message.text.split()[1] if len(message.text.split()) > 1 else None
        
        if not existing_user:
            if referral_code and referral_code != str(user_id):  # Перевірка що користувач не запросив сам себе
                referrer_id = int(referral_code)
                
                # Перевіряємо чи існує реферер
                referrer = safe_execute_sql(
                    "SELECT user_id, balance FROM users WHERE user_id = ?", 
                    (referrer_id,), 
                    fetch_one=True
                )
                
                if referrer:
                    # Додаємо нового користувача з реферером
                    safe_execute_sql(
                        """INSERT INTO users (user_id, username, referrer_id) 
                           VALUES (?, ?, ?)""",
                        (user_id, username, referrer_id)
                    )
                    
                    # Нараховуємо бонус реферу
                    new_balance = referrer[1] + REFERRAL_REWARD
                    safe_execute_sql(
                        "UPDATE users SET balance = ?, total_earnings = total_earnings + ? WHERE user_id = ?",
                        (new_balance, REFERRAL_REWARD, referrer_id)
                    )
                    
                    # Додаємо запис про реферальну транзакцію
                    safe_execute_sql(
                        """INSERT INTO transactions (user_id, amount, type, status)
                           VALUES (?, ?, 'referral_reward', 'completed')""",
                        (referrer_id, REFERRAL_REWARD)
                    )
                    
                    # Відправляємо повідомлення реферу про нового реферала
                    bot.send_message(
                        referrer_id,
                        f"🎉 У вас новый реферал! (@{username})\n"
                        f"💰 Начислено: {REFERRAL_REWARD}$\n"
                        f"💳 Ваш новый баланс: {new_balance}$"
                    )
                else:
                    # Якщо реферер не знайдений, додаємо користувача без реферера
                    safe_execute_sql(
                        "INSERT INTO users (user_id, username) VALUES (?, ?)",
                        (user_id, username)
                    )
            else:
                # Додаємо користувача без реферера
                safe_execute_sql(
                    "INSERT INTO users (user_id, username) VALUES (?, ?)",
                    (user_id, username)
                )

        # Створюємо реферальне посилання
        ref_link = f"https://t.me/{bot.get_me().username}?start={user_id}"
        
        # Отримуємо статистику рефералів
        referrals_count = safe_execute_sql(
            "SELECT COUNT(*) FROM users WHERE referrer_id = ?",
            (user_id,),
            fetch_one=True
        )[0]
        
        # Отримуємо загальний заробіток з рефералів
        total_ref_earnings = safe_execute_sql(
            """SELECT COALESCE(SUM(amount), 0) FROM transactions 
               WHERE user_id = ? AND type = 'referral_reward'""",
            (user_id,),
            fetch_one=True
        )[0]

        welcome_message = (
            f"👋 Приветстувуем в боте!\n\n"
            f"💎 За каждого приглашенного друга вы получаете {REFERRAL_REWARD}$!\n\n"
            f"🔥 Возможности:\n"
            f"💰 Зароботок на рефералах\n"
            f"💳 Вывод денег\n"
            f"📊 Статистика"
        )

        bot.send_message(user_id, welcome_message, reply_markup=create_main_keyboard(user_id))

    except Exception as e:
        error_msg = f"Ошибка в функции start: {str(e)}"
        print(error_msg)
        print(f"Referral code from message: {message.text.split()[1] if len(message.text.split()) > 1 else None}")
        bot.send_message(ADMIN_ID, f"❌ Ошибка регистрации пользователя: {str(e)}")


@bot.callback_query_handler(func=lambda call: call.data == "check_subscription")
def check_subscription_callback(call):
    user_id = call.from_user.id
    
    if check_subscription(user_id):
        try:
            # Отримуємо реферальний код з бази даних або тимчасового зберігання
            referral_data = safe_execute_sql(
                "SELECT referral_code FROM temp_referrals WHERE user_id = ?",
                (user_id,),
                fetch_one=True
            )
            
            if referral_data and referral_data[0]:
                referrer_id = int(referral_data[0])
                
                # Перевіряємо чи існує реферер
                referrer = safe_execute_sql(
                    "SELECT user_id, balance FROM users WHERE user_id = ?",
                    (referrer_id,),
                    fetch_one=True
                )
                
                # Перевіряємо чи не був цей користувач вже рефералом
                existing_referral = safe_execute_sql(
                    "SELECT id FROM referral_history WHERE referral_user_id = ?",
                    (user_id,),
                    fetch_one=True
                )
                
                if referrer and not existing_referral:
                    # Нараховуємо бонус реферу
                    new_balance = referrer[1] + REFERRAL_REWARD
                    safe_execute_sql(
                        "UPDATE users SET balance = ?, total_earnings = total_earnings + ? WHERE user_id = ?",
                        (new_balance, REFERRAL_REWARD, referrer_id)
                    )
                    
                    # Додаємо запис про транзакцію
                    safe_execute_sql(
                        """INSERT INTO transactions (user_id, amount, type, status)
                           VALUES (?, ?, 'referral_reward', 'completed')""",
                        (referrer_id, REFERRAL_REWARD)
                    )
                    
                    # Зберігаємо інформацію про реферала в історії
                    safe_execute_sql(
                        """INSERT INTO referral_history (referrer_id, referral_user_id, reward_amount)
                           VALUES (?, ?, ?)""",
                        (referrer_id, user_id, REFERRAL_REWARD)
                    )
                    
                    # Відправляємо повідомлення реферу
                    username = call.from_user.username or f"User{user_id}"
                    bot.send_message(
                        referrer_id,
                        f"🎉 У вас новый реферал! (@{username})\n"
                        f"💰 Начислено: {REFERRAL_REWARD}$\n"
                        f"💳 Ваш новый баланс: {new_balance}$"
                    )
            
            # Видаляємо тимчасові дані
            safe_execute_sql(
                "DELETE FROM temp_referrals WHERE user_id = ?",
                (user_id,)
            )
            
            # Відправляємо привітальне повідомлення
            bot.edit_message_text(
                "✅ Подписка проверена! Добро пожаловать в бот!",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
            
            # Запускаємо звичайний процес старту
            start(call.message)
            
        except Exception as e:
            print(f"Error in check_subscription_callback: {str(e)}")
            bot.send_message(ADMIN_ID, f"❌ Ошибка в обработке подписки: {str(e)}")
    
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Вы не подписались на все каналы. Проверьте подписку!"
        )


def debug_referral_system(referrer_id, new_user_id):
    """
    Функція для діагностики реферальної системи
    """
    try:
        print(f"Starting referral debug for referrer {referrer_id} and new user {new_user_id}")
        
        # Перевірка балансу рефера до
        balance_before = safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (referrer_id,),
            fetch_one=True
        )
        print(f"Referrer balance before: {balance_before}")
        
        # Перевірка транзакцій
        transactions = safe_execute_sql(
            'SELECT * FROM transactions WHERE user_id = ? AND type = ? ORDER BY created_at DESC LIMIT 5',
            (referrer_id, 'referral'),
            fetch_one=False
        )
        print(f"Recent referral transactions: {transactions}")
        
        # Перевірка зв'язку між користувачами
        referral_link = safe_execute_sql(
            'SELECT referrer_id FROM users WHERE user_id = ?',
            (new_user_id,),
            fetch_one=True
        )
        print(f"Referral link in database: {referral_link}")
        
        return {
            'balance_before': balance_before,
            'transactions': transactions,
            'referral_link': referral_link
        }
    except Exception as e:
        print(f"Error in referral debug: {str(e)}")
        return None

# Обробник текстових повідомлень
@bot.message_handler(content_types=['text'])
def handle_text(message):
    user_id = message.from_user.id
    text = message.text.strip()

    # Базові перевірки
    if message.from_user.is_bot:
        return

    if not check_subscription(user_id):
        start(message)
        return

    # Спрощений обробник адмін-панелі
    if text == '🔑 Адмін панель' or text == 'Адмін панель':
        if user_id == ADMIN_ID:
            show_admin_panel(message)
        return

    # Спрощений обробник команди видалення
    if text == '❌ Видалити користувача' and user_id == ADMIN_ID:
        start_user_deletion(message)
        return

    # Перевіряємо стан очікування ID для видалення
    try:
        state = safe_execute_sql(
            'SELECT state FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True
        )
        
        if state and state[0] == 'waiting_for_user_deletion' and user_id == ADMIN_ID:
            try:
                user_to_delete = int(text)
                handle_user_deletion(message)
                return
            except ValueError:
                bot.send_message(ADMIN_ID, "❌ Невірний формат ID користувача. Спробуйте ще раз.")
                return
    except Exception as e:
        print(f"Error checking state: {str(e)}")

    # Обробка інших команд
    if user_id == ADMIN_ID:
        admin_commands = {
            '📊 Статистика': show_statistics,
            '📢 Розсилка': start_broadcast,
            '💵 Змінити баланс': start_balance_change,
            '📁 Управління каналами': show_channel_management,
            '➕ Додати канал': start_adding_channel,
            '🎫 Статистика промокодів': show_promo_stats,
            '➕ Додати промокод': start_adding_promo,
            '🔙 Назад': back_to_main_menu
        }
        if text in admin_commands:
            admin_commands[text](message)
            return

    user_commands = {
        '💰 Баланс': show_balance,
        '👥 Реферальная система': show_referral_system,
        '💳 Вывести деньги': start_withdrawal,
        '📊 Моя статистика': show_user_statistics,
        '🍀 Промокод': handle_promo_code,
        '🏆 Таблица лидеров': show_leaders_board,
        '🛠️Тех.Поддержка': tech_support
    }
    if text in user_commands:
        user_commands[text](message)
        return
            
# Додаємо обробку стану очікування ID користувача для видалення
@bot.message_handler(func=lambda message: 
    message.from_user.id == ADMIN_ID and 
    safe_execute_sql('SELECT state FROM users WHERE user_id = ?', 
                    (ADMIN_ID,), 
                    fetch_one=True)[0] == 'waiting_for_user_deletion')
def process_user_deletion(message):
    handle_user_deletion(message)

# Функції для звичайних користувачів
def show_balance(message):
    user_id = message.from_user.id
    try:
        # Змінюємо на fetch_one=True, оскільки ми очікуємо один результат
        result = safe_execute_sql(
            'SELECT balance, total_earnings FROM users WHERE user_id = ?',
            (user_id,),
            fetch_one=True  # Додаємо цей параметр
        )

        if result:  # Тепер перевіряємо просто result
            balance, total_earnings = result
            response = (
                f"💰 Ваш баланс: {balance:.2f}$\n"
                f"📈 Общий заработок: {total_earnings:.2f}$"
            )
            bot.send_message(user_id, response)
        else:
            bot.send_message(user_id, "❌ Помилка отримання балансу")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка показу балансу для {user_id}: {str(e)}")
        print(f"Error in show_balance: {str(e)}")  # Додаємо логування помилки


def show_referral_system(message):
    user_id = message.from_user.id
    try:
        result = safe_execute_sql(
            '''SELECT COUNT(*) as ref_count,
               (SELECT SUM(amount) FROM transactions
                WHERE user_id = ? AND type = 'referral') as ref_earnings
               FROM users WHERE referrer_id = ?''',
            (user_id, user_id)
        )

        if result and result[0]:
            ref_count, ref_earnings = result[0]
            ref_earnings = ref_earnings or 0

            bot_username = bot.get_me().username
            ref_link = f"https://t.me/{bot_username}?start={user_id}"

            response = (
                f"👥 Ваша реферальная ссылка:\n{ref_link}\n\n"
                f"📊 Статистика рефералов:\n"
                f"👤 Количество рефералов: {ref_count}\n"
                f"💰 Заработок з рефералов: {ref_earnings:.2f}$\n"
                f"💵 Награда за нового реферала: {REFERRAL_REWARD}$"
            )

            keyboard = types.InlineKeyboardMarkup()
            # Додатковий код для клавіатури
            ref_button = types.InlineKeyboardButton("Поделиться", switch_inline_query=f"{ref_link}")
            keyboard.add(ref_button)

            bot.send_message(user_id, response, reply_markup=keyboard)
        else:
            bot.send_message(user_id, "❌ Помилка отримання інформації про реферальну систему")
    except Exception as e:
        bot.send_message(ADMIN_ID,
                         f"❌ Помилка отримання інформації про реферальну систему для користувача {user_id}: {str(e)}")


def create_admin_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        '📊 Статистика',
        '📢 Розсилка',
        '💵 Змінити баланс',
        '📁 Управління каналами',
        '➕ Додати канал',
        '🎫 Статистика промокодів',
        '➕ Додати промокод',
        '❌ Видалити користувача',  # Нова кнопка
        '🔙 Назад'
    ]
    keyboard.add(*buttons)
    bot.send_message(message.chat.id, "🔑 Адмін-панель", reply_markup=keyboard)


def show_statistics(message):
    try:
        users = safe_execute_sql('SELECT COUNT(*) FROM users')
        total_earnings = safe_execute_sql('SELECT SUM(total_earnings) FROM users')
        total_withdrawals = safe_execute_sql("SELECT COUNT(*) FROM transactions WHERE type = 'withdrawal'")
        withdrawal_sum = safe_execute_sql("SELECT SUM(amount) FROM transactions WHERE type = 'withdrawal'")

        response = (
            f"📊 Статистика бота:\n"
            f"👥 Кількість користувачів: {users[0][0]}\n"
            f"💸 Загальна сума заробітку: {total_earnings[0][0] if total_earnings[0][0] else 0:.2f}$\n"
            f"📤 Кількість заявок на виведення: {total_withdrawals[0][0]}\n"
            f"💵 Сума заявок на виведення: {withdrawal_sum[0][0] if withdrawal_sum[0][0] else 0:.2f}$"
        )

        bot.send_message(ADMIN_ID, response)
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка отримання статистики: {str(e)}")


def show_channel_management(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        bot.send_message(user_id, "У вас немає доступу до цього розділу.")
        return

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(types.KeyboardButton('➕ Додати канал'))
    keyboard.add(types.KeyboardButton('🔙 Назад'))

    bot.send_message(user_id, "Управління каналами", reply_markup=keyboard)

def start_adding_channel(message):
    user_id = message.from_user.id
    bot.send_message(user_id, "Введіть посилання на канал або тег (наприклад, @назватегу або https://t.me/назватегу):")
    safe_execute_sql(
        'UPDATE users SET state = ? WHERE user_id = ?',
        (UserState.waiting_for_channel_add, user_id)
    )

@bot.message_handler(func=lambda message: get_user_state(message.from_user.id) == UserState.waiting_for_channel_add)
async def process_new_channel(message):
    user_id = message.from_user.id
    try:
        channel_input = message.text.strip()

        # Обробка формату введення
        if channel_input.startswith('@'):
            channel_tag = channel_input
        elif channel_input.startswith('https://t.me/') or channel_input.startswith('t.me/'):
            channel_tag = '@' + channel_input.split('/')[-1]
        else:
            bot.send_message(user_id, "❌ Невірний формат. Введіть тег каналу або посилання на канал.")
            return

        # Перевірка прав бота в каналі
        try:
            chat_info = await bot.get_chat(channel_tag)
            bot_member = await bot.get_chat_member(chat_info.id, bot.get_me().id)

            if bot_member.status not in ['administrator', 'creator']:
                bot.send_message(user_id,
                                 "❌ Бот повинен бути адміністратором каналу. Додайте бота як адміністратора та спробуйте ще раз.")
                return

            # Отримуємо додаткову інформацію про канал
            channel_name = chat_info.title
            channel_link = chat_info.username and f"https://t.me/{chat_info.username}" or f"https://t.me/+{chat_info.invite_link}"
            channel_id = str(chat_info.id)

            # Перевіряємо чи канал вже існує
            existing_channel = safe_execute_sql(
                'SELECT channel_id FROM channels WHERE channel_id = ?',
                (channel_id,)
            )

            if existing_channel:
                bot.send_message(user_id, "❌ Цей канал вже додано до бази даних.")
                return

            # Додаємо канал до бази даних
            safe_execute_sql(
                'INSERT INTO channels (channel_id, channel_name, channel_link, is_required) VALUES (?, ?, ?, ?)',
                (channel_id, channel_name, channel_link, 1)
            )

            bot.send_message(user_id, f"✅ Канал {channel_name} успішно додано!")

        except Exception as e:
            bot.send_message(user_id,
                             "❌ Не вдалося отримати інформацію про канал. Переконайтесь, що:\n1. Бот доданий до каналу\n2. Бот має права адміністратора\n3. Посилання/тег каналу правильні")
            bot.send_message(ADMIN_ID, f"Помилка при додаванні каналу: {str(e)}")

    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка додавання каналу: {str(e)}")
        bot.send_message(user_id, "❌ Сталася помилка при додаванні каналу. Спробуйте ще раз пізніше.")
    finally:
        # Повертаємо користувача до початкового стану
        safe_execute_sql(
            'UPDATE users SET state = ? WHERE user_id = ?',
            (UserState.none, user_id)
        )


def get_user_state(user_id):
    result = safe_execute_sql(
        'SELECT state FROM users WHERE user_id = ?',
        (user_id,)
    )
    if result and result[0]:
        return result[0][0]
    return UserState.none


def start_withdrawal(message):
    user_id = message.from_user.id
    try:
        result = safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,)
        )

        if result and result[0]:
            balance = result[0][0]

            if balance >= MIN_WITHDRAWAL:
                msg = (
                    f"💳 Вывести деньги\n\n"
                    f"💰 Ваш баланс: {balance:.2f}$\n"
                    f"💵 Минимальная сумма: {MIN_WITHDRAWAL}$\n\n"
                    f"Введите сумму для вывода:"
                )
                bot.send_message(user_id, msg)
                bot.register_next_step_handler(message, process_withdrawal_amount)
            else:
                bot.send_message(
                    user_id,
                    f"❌ Недостаточно средств!\n💰 Минимальная сумма: {MIN_WITHDRAWAL}$"
                )
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка початку виведення для {user_id}: {str(e)}")


def process_withdrawal_amount(message):
    user_id = message.from_user.id
    try:
        amount = float(message.text)

        if amount < MIN_WITHDRAWAL:
            bot.send_message(
                user_id,
                f"❌ Минимальная сумма для вывода: {MIN_WITHDRAWAL}$"
            )
            return

        # Запитуємо TON гаманець
        msg = f"✅ Сума {amount:.2f}$ прийнята.\nВведите ваш TON кошелек:"
        bot.send_message(user_id, msg)
        bot.register_next_step_handler(message, lambda m: process_withdrawal_wallet(m, amount))
    except ValueError:
        bot.send_message(user_id, "❌ Введите корректную сумму!")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка обробки суми виведення для {user_id}: {str(e)}")

def process_withdrawal_wallet(message, amount):
    user_id = message.from_user.id
    ton_wallet = message.text.strip()

    try:
        result = safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,)
        )

        if result and result[0]:
            balance = result[0][0]

            if balance >= amount:
                # Створюємо транзакцію з TON гаманцем
                safe_execute_sql(
                    '''INSERT INTO transactions (user_id, amount, type, status, ton_wallet)
                       VALUES (?, ?, 'withdrawal', 'pending', ?)''',
                    (user_id, amount, ton_wallet)
                )

                # Оновлюємо баланс
                safe_execute_sql(
                    'UPDATE users SET balance = balance - ? WHERE user_id = ?',
                    (amount, user_id)
                )

                # Повідомлення користувачу і адміну
                bot.send_message(
                    user_id,
                    f"✅ Заявка на вывод {amount:.2f}$ прийнята!"
                )

                admin_msg = (
                    f"💳 Нова заявка на виведення!\n\n"
                    f"👤 Користувач: {user_id}\n"
                    f"💰 Сума: {amount:.2f}$\n"
                    f"🔑 TON кошелек: {ton_wallet}"
                )

                keyboard = types.InlineKeyboardMarkup()
                approve_button = types.InlineKeyboardButton(
                    "✅ Підтвердити",
                    callback_data=f"approve_withdrawal_{user_id}_{amount}"
                )
                reject_button = types.InlineKeyboardButton(
                    "❌ Відхилити",
                    callback_data=f"reject_withdrawal_{user_id}_{amount}"
                )
                keyboard.add(approve_button, reject_button)

                bot.send_message(ADMIN_ID, admin_msg, reply_markup=keyboard)
            else:
                bot.send_message(user_id, "❌ Недостаточно средств!")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка обробки виведення для {user_id}: {str(e)}")


# Функції для адміністратора
def show_admin_panel(message):
    if message.from_user.id != ADMIN_ID:
        return
        
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    admin_buttons = [
        '📊 Статистика',
        '📢 Розсилка',
        '💵 Змінити баланс',
        '📁 Управління каналами',
        '➕ Додати канал',
        '🎫 Статистика промокодів',
        '➕ Додати промокод',
        '❌ Видалити користувача',
        '🔙 Назад'
    ]
    for button in admin_buttons:
        keyboard.add(types.KeyboardButton(button))
    
    bot.send_message(message.chat.id, "🔑 Адмін-панель", reply_markup=keyboard)

def show_statistics(message):
    if message.from_user.id != ADMIN_ID:
        return

    try:
        stats = safe_execute_sql('''
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
                f"📊 Загальна статистика:\n\n"
                f"👥 Користувачів: {total_users}\n"
                f"💰 Загальний баланс: {total_balance:.2f}$\n"
                f"👤 Активних рефералів: {total_referrers}\n"
                f"💳 Очікують виведення: {pending_withdrawals}\n\n"
                f"Останні реєстрації:"
            )

            # Отримуємо останні реєстрації
            recent_users = safe_execute_sql('''
                SELECT user_id, username, join_date
                FROM users
                ORDER BY join_date DESC
                LIMIT 5
            ''')

            if recent_users:
                for user in recent_users:
                    response += f"\n👤 {user[1] or user[0]} - {user[2]}"

            bot.send_message(ADMIN_ID, response)
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка отримання статистики: {str(e)}")


def start_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return

    bot.send_message(ADMIN_ID, "📨 Введіть текст розсилки:")
    bot.register_next_step_handler(message, process_broadcast)


def process_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return

    broadcast_text = message.text
    users = safe_execute_sql('SELECT user_id FROM users')

    if not users:
        bot.send_message(ADMIN_ID, "❌ Немає користувачів для розсилки")
        return

    success = 0
    failed = 0

    progress_msg = bot.send_message(ADMIN_ID, "📨 Розсилка розпочата...")

    for user in users:
        try:
            bot.send_message(user[0], broadcast_text)
            success += 1

            if (success + failed) % 10 == 0:
                bot.edit_message_text(
                    f"📨 Розсилка в процесі...\n✅ Успішно: {success}\n❌ Невдало: {failed}",
                    ADMIN_ID,
                    progress_msg.message_id
                )
        except Exception:
            failed += 1
            continue

    bot.edit_message_text(
        f"📨 Розсилка завершена!\n✅ Успішно: {success}\n❌ Невдало: {failed}",
        ADMIN_ID,
        progress_msg.message_id
    )


# Колбек-обробники
@bot.callback_query_handler(func=lambda call: True)
def handle_callback_query(call):
    if "check_subscription" in call.data:
        check_user_subscription(call)
    elif "approve_withdrawal" in call.data:
        handle_withdrawal_approval(call)
    elif "reject_withdrawal" in call.data:
        handle_withdrawal_rejection(call)


def handle_withdrawal_approval(call):
    if call.from_user.id != ADMIN_ID:
        return

    try:
        _, user_id, amount = call.data.split('_')[1:]
        user_id = int(user_id)
        amount = float(amount)

        # Оновлюємо статус транзакції
        safe_execute_sql(
            '''UPDATE transactions
               SET status = 'completed'
               WHERE user_id = ? AND amount = ? AND status = 'pending'
               ORDER BY created_at DESC LIMIT 1''',
            (user_id, amount)
        )

        bot.edit_message_text(
            f"✅ Виведення {amount}$ для користувача {user_id} підтверджено!",
            call.message.chat.id,
            call.message.message_id
        )

        bot.send_message(
            user_id,
            f"✅ Ваша заявка на вывод {amount}$ подтверджена!"
        )
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка підтвердження виведення: {str(e)}")


def handle_withdrawal_rejection(call):
    if call.from_user.id != ADMIN_ID:
        return

    try:
        _, user_id, amount = call.data.split('_')[1:]
        user_id = int(user_id)
        amount = float(amount)

        # Оновлюємо статус транзакції
        safe_execute_sql(
            '''UPDATE transactions
               SET status = 'rejected'
               WHERE user_id = ? AND amount = ? AND status = 'pending'
               ORDER BY created_at DESC LIMIT 1''',
            (user_id, amount)
        )

        # Повертаємо кошти на баланс користувача
        safe_execute_sql(
            'UPDATE users SET balance = balance + ? WHERE user_id = ?',
            (amount, user_id)
        )

        bot.edit_message_text(
            f"❌ Виведення {amount}$ для користувача {user_id} відхилено!",
            call.message.chat.id,
            call.message.message_id
        )

        bot.send_message(
            user_id,
            f"❌ Ваша заявка на вывод {amount}$ была отклонена.\n💰 Средства возвращены на баланс."
        )
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка відхилення виведення: {str(e)}")


def show_user_statistics(message):
    user_id = message.from_user.id
    try:
        # Отримуємо кількість рефералів
        referrals_count = safe_execute_sql(
            '''SELECT COUNT(*) 
               FROM users 
               WHERE referrer_id = ?''',
            (user_id,),
            fetch_one=True
        )[0]

        # Отримуємо заробіток з рефералів
        ref_earnings = safe_execute_sql(
            '''SELECT COALESCE(SUM(amount), 0) 
               FROM transactions 
               WHERE user_id = ? AND type = 'referral_reward' ''',
            (user_id,),
            fetch_one=True
        )[0]

        # Отримуємо дату приєднання для розрахунку днів у боті
        join_date = safe_execute_sql(
            '''SELECT join_date 
               FROM users 
               WHERE user_id = ?''',
            (user_id,),
            fetch_one=True
        )[0]

        # Розраховуємо кількість днів у боті
        from datetime import datetime
        join_datetime = datetime.strptime(join_date, '%Y-%m-%d %H:%M:%S')
        days_in_bot = (datetime.now() - join_datetime).days

        # Формуємо повідомлення
        response = (
            f"📊 Ваша статистика:\n\n"
            f"👥 Количество рефералов: {referrals_count}\n"
            f"💰 Заработок с рефералов: {ref_earnings:.2f}$\n"
            f"⏳ Дней в боте: {days_in_bot}"
        )
        
        bot.send_message(user_id, response)
        
    except Exception as e:
        print(f"Помилка у функції статистики: {str(e)}")
        bot.send_message(ADMIN_ID, f"❌ Помилка показу статистики для {user_id}: {str(e)}")
        bot.send_message(user_id, "❌ Произошла ошибка при получении статистики")

def back_to_main_menu(message):
    user_id = message.from_user.id

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        '💰 Баланс',
        '👥 Реферальная система',
        '💳 Вывести деньги',
        '📊 Моя статистика',
        '🍀 Промокод',
        '🏆 Таблица лидеров',
        '🛠️Тех.Поддержка'
    ]

    # Додаємо кнопки адмін-панелі для адміністратора
    if user_id == ADMIN_ID:
        buttons.append('🔑 Адмін панель')

    keyboard.add(*buttons)
    bot.send_message(user_id, "📱 Головне меню:", reply_markup=keyboard)


def handle_channel_callback(call):
    print(f"Callback received: {call.data}")  # Додаємо логування

    if call.from_user.id != ADMIN_ID:
        print(f"User {call.from_user.id} is not admin")  # Логування для перевірки ID
        return

    if call.data == 'add_channel':
        print("Processing add_channel command")  # Додаємо логування
        bot.answer_callback_query(call.id)
        msg = bot.send_message(ADMIN_ID, "📢 Перешліть повідомлення з каналу:")
        bot.register_next_step_handler(msg, process_new_channel)  # Змінюємо call.message на msg

# Функції для управління каналами
def show_channel_management(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        bot.send_message(user_id, "У вас немає доступу до цього розділу.")
        return

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(types.KeyboardButton('➕ Додати канал'))
    keyboard.add(types.KeyboardButton(' Назад'))

    bot.send_message(user_id, "Управління каналами", reply_markup=keyboard)


# Функції для обробки платіжних систем
def start_balance_change(message):
    if message.from_user.id != ADMIN_ID:
        return

    bot.send_message(
        ADMIN_ID,
        "👤 Введіть ID користувача:"
    )
    bot.register_next_step_handler(message, process_user_id_for_balance)


def process_user_id_for_balance(message):
    if message.from_user.id != ADMIN_ID:
        return

    try:
        user_id = int(message.text)

        result = safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,)
        )

        if result and result[0]:
            current_balance = result[0][0]

            msg = (
                f"👤 Користувач: {user_id}\n"
                f"💰 Поточний баланс: {current_balance}$\n\n"
                f"Введіть нову суму балансу:"
            )

            bot.send_message(ADMIN_ID, msg)
            bot.register_next_step_handler(message, process_new_balance, user_id)
        else:
            bot.send_message(ADMIN_ID, "❌ Користувача не знайдено!")
    except ValueError:
        bot.send_message(ADMIN_ID, "❌ Введіть коректний ID!")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка обробки ID: {str(e)}")


def process_new_balance(message, user_id):
    if message.from_user.id != ADMIN_ID:
        return

    try:
        new_balance = float(message.text)

        if new_balance < 0:
            bot.send_message(ADMIN_ID, "❌ Баланс не може бути від'ємним!")
            return

        # Отримуємо старий баланс
        result = safe_execute_sql(
            'SELECT balance FROM users WHERE user_id = ?',
            (user_id,)
        )

        if result and result[0]:
            old_balance = result[0][0]

            # Оновлюємо баланс
            safe_execute_sql(
                'UPDATE users SET balance = ? WHERE user_id = ?',
                (new_balance, user_id)
            )

            # Додаємо транзакцію
            amount_change = new_balance - old_balance
            transaction_type = 'bonus' if amount_change > 0 else 'penalty'

            safe_execute_sql(
                '''INSERT INTO transactions (user_id, amount, type, status)
                   VALUES (?, ?, ?, 'completed')''',
                (user_id, abs(amount_change), transaction_type)
            )

            # Повідомлення адміну і користувачу
            admin_msg = (
                f"✅ Баланс користувача {user_id} змінено:\n"
                f"Було: {old_balance}$\n"
                f"Стало: {new_balance}$\n"
                f"Різниця: {amount_change:+}$"
            )
            bot.send_message(ADMIN_ID, admin_msg)

            user_msg = (
                f"💰 Ваш баланс изменен:\n"
                f"Было: {old_balance}$\n"
                f"Стало: {new_balance}$\n"
                f"Разница: {amount_change:+}$"
            )
            bot.send_message(user_id, user_msg)
        else:
            bot.send_message(ADMIN_ID, "❌ Користувача не знайдено!")
    except ValueError:
        bot.send_message(ADMIN_ID, "❌ Введіть коректну суму!")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Помилка зміни балансу: {str(e)}")


# Функції для роботи з промокодами
def start_adding_promo(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    bot.send_message(user_id,
                     "Введіть промокод, суму винагороди та максимальну кількість активацій через пробіл\nНаприклад: HAPPY2024 100 50")
    bot.register_next_step_handler(message, process_promo_code_addition)


def process_promo_code_addition(message):
    try:
        promo_code, reward, max_activations = message.text.split()
        reward = float(reward)
        max_activations = int(max_activations)

        # Додаємо промокод в базу даних
        add_promo_code(promo_code, reward, max_activations)

        bot.send_message(message.from_user.id,
                         f"✅ Промокод успішно додано!\nКод: {promo_code}\nСума: {reward}\nМакс. активацій: {max_activations}")
    except ValueError:
        bot.send_message(message.from_user.id, "❌ Неправильний формат! Спробуйте ще раз.")
    except Exception as e:
        bot.send_message(message.from_user.id, f"❌ Помилка: {str(e)}")


def add_promo_code(code, reward, max_activations):
    safe_execute_sql(
        'INSERT INTO promo_codes (code, reward, max_activations, current_activations) VALUES (?, ?, ?, 0)',
        (code, reward, max_activations)
    )


def handle_promo_code(message):
    user_id = message.from_user.id
    if message.text.strip() == '🍀 Промокод':
        bot.send_message(user_id, "Введите промокод:", reply_markup=types.ForceReply())
        bot.register_next_step_handler(message, process_promo_activation)


def process_promo_activation(message):
    user_id = message.from_user.id
    promo_code = message.text.strip().upper()

    try:
        # Перевіряємо, чи промокод існує
        promo = safe_execute_sql(
            "SELECT code, reward, max_activations, current_activations FROM promo_codes WHERE code = ?", 
            (promo_code,), 
            fetch_one=True
        )
        
        if not promo:
            bot.send_message(user_id, "❌ Такого промокода не существует!")
            return

        # Розпаковуємо дані
        code, reward, max_activations, current_activations = promo
        
        # Перевіряємо чи не перевищено ліміт активацій
        if current_activations >= max_activations:
            bot.send_message(user_id, "❌ Промокод больше не действителен (достигнут лимит активаций)!")
            return

        # Перевіряємо, чи користувач вже використовував цей промокод
        used = safe_execute_sql(
            "SELECT 1 FROM used_promo_codes WHERE user_id = ? AND promo_code = ?",
            (user_id, promo_code),
            fetch_one=True
        )
        if used:
            bot.send_message(user_id, "❌ Вы уже использовали этот промокод!")
            return

        # Якщо всі перевірки пройдені, додаємо запис в таблицю used_promo_codes
        safe_execute_sql(
            "INSERT INTO used_promo_codes (user_id, promo_code) VALUES (?, ?)",
            (user_id, promo_code)
        )

        # Оновлюємо кількість активацій промокоду
        safe_execute_sql(
            "UPDATE promo_codes SET current_activations = current_activations + 1 WHERE code = ?",
            (promo_code,)
        )

        # Оновлюємо баланс користувача
        current_balance = safe_execute_sql(
            "SELECT balance FROM users WHERE user_id = ?",
            (user_id,),
            fetch_one=True
        )[0]

        new_balance = current_balance + reward
        safe_execute_sql(
            "UPDATE users SET balance = ? WHERE user_id = ?",
            (new_balance, user_id)
        )

        bot.send_message(
            user_id,
            f"✅ Промокод успешно активирован!\n"
            f"💰 Начислено: {reward} $\n"
            f"💳 Ваш баланс: {new_balance} $"
        )

    except Exception as e:
        print(f"Помилка при активації промокоду: {str(e)}")
        bot.send_message(user_id, "❌ Произошла ошибка при активации промокода. Попробуйте позже.")


# Додамо команду для перегляду статистики промокодів для адміна
def show_promo_stats(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return

    try:
        promo_stats = safe_execute_sql(
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
            bot.send_message(user_id, "📊 Промокодов еще не создано")
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

        bot.send_message(user_id, response)

    except Exception as e:
        bot.send_message(user_id, "❌ Ошибка при получении статистики промокодов")
        print(f"Ошибка show_promo_stats: {str(e)}")


@bot.message_handler(func=lambda message: message.text == "🏆 Таблиця лідерів")
def show_leaders_board(message):
    try:
        # Підключення до бази даних
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        # Запит для отримання топ-10 користувачів за кількістю рефералів
        cursor.execute('''
            SELECT 
                user_id, 
                username, 
                (SELECT COUNT(*) FROM users u2 WHERE u2.referrer_id = u1.user_id) as referral_count,
                balance
            FROM 
                users u1
            ORDER BY 
                referral_count DESC
            LIMIT 10
        ''')

        leaders = cursor.fetchall()
        conn.close()

        # Формування повідомлення
        if leaders:
            response = "🏆 Топ-10 лидеров по количеству рефералов:\n\n"
            for index, (user_id, username, referral_count, balance) in enumerate(leaders, 1):
                # Форматування імені користувача
                display_name = username if username else f"Пользователь {user_id}"

                response += (f"{index}. {display_name}\n"
                             f"   📊 Реферали: {referral_count}\n"
                             f"   💰 Баланс: ${balance:.2f}\n\n")
        else:
            response = "🏆 Пока нет лидеров.Приглашайте друзей!"

        # Надсилання повідомлення БЕЗ parse_mode
        bot.send_message(message.chat.id, response)

    except Exception as e:
        print(f"Помилка при показі таблиці лідерів: {e}")
        bot.send_message(message.chat.id, "❌ Не вдалося показати таблицю лідерів. Спробуйте пізніше.")


@bot.message_handler(func=lambda message: message.text == '🛠️Тех.Поддержка')
def tech_support(message):
    support_link = "tg://resolve?domain=m1sepf"
    bot.send_message(message.chat.id, "Перенаправляем к поддержке...",
                     reply_markup=types.InlineKeyboardMarkup().add(
                         types.InlineKeyboardButton("📞 Написать поддержке", url=support_link)
                     ))

# Додаємо функцію для видалення користувача
def delete_user_from_database(user_id):
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()
        
        # Видаляємо всі транзакції користувача
        cursor.execute('DELETE FROM transactions WHERE user_id = ?', (user_id,))
        
        # Видаляємо використані промокоди користувача
        cursor.execute('DELETE FROM used_promo_codes WHERE user_id = ?', (user_id,))
        
        # Видаляємо самого користувача
        cursor.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deleting user: {str(e)}")
        return False

# Функція для початку процесу видалення
def start_user_deletion(message):
    if message.from_user.id != ADMIN_ID:
        return

    # Оновлюємо стан користувача
    safe_execute_sql(
        'UPDATE users SET state = ? WHERE user_id = ?',
        ('waiting_for_user_deletion', ADMIN_ID)
    )
    
    bot.send_message(ADMIN_ID, "👤 Введіть ID користувача, якого потрібно видалити:")

# Додаємо обробник для видалення користувача
def handle_user_deletion(message):
    try:
        user_to_delete = int(message.text)
        
        # Перевіряємо чи існує користувач
        user_exists = safe_execute_sql(
            'SELECT username FROM users WHERE user_id = ?',
            (user_to_delete,),
            fetch_one=True
        )
        
        if not user_exists:
            bot.send_message(ADMIN_ID, "❌ Користувач не знайдений в базі даних.")
        else:
            if delete_user_from_database(user_to_delete):
                bot.send_message(
                    ADMIN_ID,
                    f"✅ Користувач {user_exists[0] or user_to_delete} успішно видалений з бази даних."
                )
            else:
                bot.send_message(ADMIN_ID, "❌ Помилка при видаленні користувача.")
        
        # Скидаємо стан і повертаємо до адмін-панелі
        safe_execute_sql(
            'UPDATE users SET state = ? WHERE user_id = ?',
            ('none', ADMIN_ID)
        )
        show_admin_panel(message)
        
    except ValueError:
        bot.send_message(ADMIN_ID, "❌ Невірний формат ID користувача. Спробуйте ще раз.")

# Функція для безпечного виконання SQL-запитів
def safe_execute_sql(query, params=None, fetch_one=False):
    try:
        conn = sqlite3.connect('bot_database.db')
        cursor = conn.cursor()

        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        if fetch_one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()

        conn.commit()
        conn.close()
        return result
    except Exception as e:
        print(f"Database error: {str(e)}")
        return None


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


# Запуск бота
if __name__ == "__main__":
    try:
        ensure_database_exists()
        init_db()  # Викликаємо функцію для створення всіх таблиць

        # Спочатку створюємо таблицю, якщо вона не існує
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                channel_id TEXT,
                channel_name TEXT,
                channel_link TEXT,
                is_required INTEGER DEFAULT 1,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()

        # Потім вже перевіряємо структуру
        cursor.execute("PRAGMA table_info(channels)")
        structure = cursor.fetchall()
        print(f"Структура таблиці channels: {structure}")
        
        # Додавання тестового каналу
        safe_execute_sql('''
            INSERT OR IGNORE INTO channels (channel_id, channel_name, channel_link, is_required)
            VALUES (?, ?, ?, ?)
        ''', ('-1002157115077', 'CryptoWave', 'https://t.me/CryptoWaveee', 1))
        
        # Перевіряємо структуру таблиць
        check_table_structure()

        print("🚀 Бот запущений...")
        bot.polling(none_stop=True)
    except Exception as e:
        print(f"❌ Помилка запуску бота: {str(e)}")
