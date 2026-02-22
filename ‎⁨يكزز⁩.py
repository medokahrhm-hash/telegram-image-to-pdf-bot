import os
from dotenv import load_dotenv
load_dotenv()
BOT_TOKEN = os.environ.get('BOT_TOKEN')
OWNER_ID = int(os.environ.get('OWNER_ID', 0))
import sqlite3
import logging
import pandas as pd
import io
import datetime
import secrets
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, User
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from flask import Flask
from threading import Thread
import time
from keep_alive import keep_alive

# --- الإعدادات ----
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- خادم Flask للحفاظ على البوت نشطاً (اختياري، يمكن تعطيله) ---
app_flask = Flask(__name__)

@app_flask.route('/')
def home():
    return "Bot is Running!"

@app_flask.route('/health')
def health():
    return "OK", 200

def run_flask():
    try:
        app_flask.run(host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"خطأ في تشغيل Flask: {e}")

def keep_alive():
    flask_thread = Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("تم تشغيل خادم Flask للحفاظ على البوت نشطاً")

# --- قاعدة البيانات ---
def get_db():
    return sqlite3.connect('quiz_system.db', check_same_thread=False, timeout=20)

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, full_name TEXT, username TEXT, joined_at TIMESTAMP)')

    c.execute('''CREATE TABLE IF NOT EXISTS quizzes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        is_active INTEGER DEFAULT 0,
        private_token TEXT UNIQUE,
        max_users INTEGER DEFAULT 0,
        used_users INTEGER DEFAULT 0
    )''')

    c.execute('CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY AUTOINCREMENT, quiz_id INTEGER, file_name TEXT)')
    c.execute('''CREATE TABLE IF NOT EXISTS questions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quiz_id INTEGER,
        group_id INTEGER,
        stem TEXT,
        a TEXT,
        b TEXT,
        c TEXT,
        d TEXT,
        correct TEXT,
        explanation TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS progress (
        user_id INTEGER,
        quiz_id INTEGER,
        current_grp_id INTEGER,
        current_q_idx INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, quiz_id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS private_access (
        user_id INTEGER,
        quiz_id INTEGER,
        accessed_at TIMESTAMP,
        PRIMARY KEY (user_id, quiz_id)
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')

    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('required_channel', ''))
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('channel_link', ''))
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('bot_active', '1'))
    c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", ('show_channel_link', '1'))

    try:
        c.execute("SELECT fail_count FROM users LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE users ADD COLUMN fail_count INTEGER DEFAULT 0")
            conn.commit()
            logger.info("تم إضافة عمود fail_count لجدول المستخدمين")
        except sqlite3.OperationalError:
            pass

    try:
        c.execute("SELECT private_token FROM quizzes LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE quizzes ADD COLUMN private_token TEXT")
            conn.commit()
            logger.info("تم إضافة عمود private_token")
        except sqlite3.OperationalError:
            pass

    try:
        c.execute("SELECT max_users FROM quizzes LIMIT 1")
    except sqlite3.OperationalError:
        try:
            c.execute("ALTER TABLE quizzes ADD COLUMN max_users INTEGER DEFAULT 0")
            c.execute("ALTER TABLE quizzes ADD COLUMN used_users INTEGER DEFAULT 0")
            conn.commit()
            logger.info("تم إضافة أعمدة التحكم في عدد المستخدمين")
        except sqlite3.OperationalError:
            pass

    conn.close()
    logger.info("تم تهيئة قاعدة البيانات بنجاح")

# --- دوال مساعدة للإعدادات ---
def get_setting(key: str) -> str:
    conn = get_db()
    try:
        result = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return result[0] if result else ''
    finally:
        conn.close()

def update_setting(key: str, value: str):
    conn = get_db()
    try:
        conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
        conn.commit()
    finally:
        conn.close()

# --- وظائف المساعدة للرابط الخاص ---
def can_access_private(user_id, quiz_id, conn):
    quiz = conn.execute('SELECT max_users, used_users FROM quizzes WHERE id=?', (quiz_id,)).fetchone()
    if not quiz:
        return False, "الاختبار غير موجود"
    max_users, used_users = quiz
    existing = conn.execute('SELECT 1 FROM private_access WHERE user_id=? AND quiz_id=?', (user_id, quiz_id)).fetchone()
    if existing:
        return True, "مسموح (مسجل مسبقاً)"
    if max_users == 0:
        return True, "مسموح (غير محدود)"
    if used_users < max_users:
        return True, "مسموح"
    else:
        return False, f"عذراً، العدد الأقصى للمستخدمين لهذا الرابط هو {max_users} وقد اكتمل."

def register_private_access(user_id, quiz_id, conn):
    conn.execute('INSERT OR IGNORE INTO private_access (user_id, quiz_id, accessed_at) VALUES (?,?,?)',
                 (user_id, quiz_id, datetime.datetime.now()))
    conn.execute('''UPDATE quizzes SET used_users = (
        SELECT COUNT(*) FROM private_access WHERE quiz_id=?
    ) WHERE id=?''', (quiz_id, quiz_id))
    conn.commit()

# --- دالة التحقق من الاشتراك (معدلة لاستقبال كائن user) ---
async def check_subscription(user: User, context: ContextTypes.DEFAULT_TYPE) -> bool:
    required_channel = get_setting('required_channel')
    if not required_channel:
        return True

    try:
        member = await context.bot.get_chat_member(
            chat_id=required_channel.strip(),
            user_id=user.id
        )
        if member.status in ['member', 'administrator', 'creator']:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"خطأ في التحقق من الاشتراك للمستخدم {user.id}: {e}")
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=(
                    f"⚠️ حدث خطأ في التحقق من الاشتراك\n"
                    f"المستخدم: {user.full_name}\n"
                    f"المعرف: {user.id}\n"
                    f"يوزر: @{user.username if user.username else 'لا يوجد'}\n"
                    f"القناة المطلوبة: {required_channel}\n"
                    f"الخطأ: {e}"
                )
            )
        except:
            pass
        return False

# --- دالة التحقق من حالة البوت (نشط/متوقف) ---
async def is_bot_active_for_user(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    bot_active = get_setting('bot_active')
    return bot_active == '1'

# --- وظائف المستخدم ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not await is_bot_active_for_user(user.id):
        await update.message.reply_text("البوت تحت الصيانة، حاول مرة اخرى لاحقاّ.")
        return

    conn = get_db()

    existing = conn.execute('SELECT 1 FROM users WHERE user_id=?', (user.id,)).fetchone()
    if not existing:
        conn.execute('INSERT INTO users (user_id, full_name, username, joined_at) VALUES (?,?,?,?)',
                     (user.id, user.full_name, user.username, datetime.datetime.now()))
        conn.commit()
        count = conn.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        msg = (f"🔔 عضو جديد انضم:\n👤 الاسم: {user.full_name}\n🆔 الآيدي: `{user.id}`\n🔗 يوزر: @{user.username or 'None'}\n🔢 التسلسل: {count}")
        await context.bot.send_message(chat_id=OWNER_ID, text=msg, parse_mode='Markdown')

    if context.args:
        token = context.args[0]
        quiz = conn.execute('SELECT id, name, max_users, used_users FROM quizzes WHERE private_token=?', (token,)).fetchone()
        if quiz:
            quiz_id, quiz_name, max_users, used_users = quiz
            allowed, msg = can_access_private(user.id, quiz_id, conn)
            if allowed:
                if not await check_subscription(user, context):
                    conn.close()
                    channel_link = get_setting('channel_link')
                    show_link = get_setting('show_channel_link')
                    keyboard = []
                    if show_link == '1' and channel_link:
                        keyboard.append([InlineKeyboardButton("📢 اشترك في القناة", url=channel_link)])
                    await update.message.reply_text(
                        "❌ عذراً، للوصول إلى هذا الاختبار يجب أن تكون مشتركاً في قناتنا أولاً.\n"
                        "يرجى الاشتراك ثم حاول مرة أخرى.",
                        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
                    )
                    return
                register_private_access(user.id, quiz_id, conn)
                conn.close()
                await update.message.reply_text(f"🔑 تم منحك وصولاً خاصاً لاختبار: **{quiz_name}**", parse_mode='Markdown')
                return await send_next_ui(update, context, user.id, quiz_id, reset_progress=False)
            else:
                conn.close()
                await update.message.reply_text(f"❌ {msg}")
                return
        else:
            conn.close()
            await update.message.reply_text("❌ رابط غير صالح.")
            return

    quizzes = conn.execute('SELECT id, name FROM quizzes WHERE is_active=1').fetchall()
    conn.close()
    if not quizzes:
        await update.message.reply_text("👋 لا توجد اختبارات نشطة حالياً.")
    else:
        btns = [[InlineKeyboardButton(q[1], callback_data=f"startquiz_{q[0]}")] for q in quizzes]
        await update.message.reply_text("📚 الاختبارات المتاحة:", reply_markup=InlineKeyboardMarkup(btns))

# --- منطق الأسئلة المتسلسل ---
async def get_question_data(user_id, quiz_id, reset=False):
    conn = get_db()
    try:
        if reset:
            conn.execute('DELETE FROM progress WHERE user_id=? AND quiz_id=?', (user_id, quiz_id))
            conn.commit()
            prog = None
        else:
            prog = conn.execute('SELECT current_grp_id, current_q_idx FROM progress WHERE user_id=? AND quiz_id=?', (user_id, quiz_id)).fetchone()

        if not prog:
            first_grp = conn.execute('SELECT id, file_name FROM groups WHERE quiz_id=? ORDER BY id LIMIT 1', (quiz_id,)).fetchone()
            if not first_grp:
                return None, None, None, None
            conn.execute('INSERT OR REPLACE INTO progress (user_id, quiz_id, current_grp_id, current_q_idx) VALUES (?,?,?,0)',
                         (user_id, quiz_id, first_grp[0]))
            conn.commit()
            grp_id, idx, grp_name = first_grp[0], 0, first_grp[1]
        else:
            grp_id, idx = prog
            grp_name = conn.execute('SELECT file_name FROM groups WHERE id=?', (grp_id,)).fetchone()[0]

        questions = conn.execute('SELECT * FROM questions WHERE group_id=? ORDER BY id', (grp_id,)).fetchall()
        return questions, grp_id, idx, grp_name
    finally:
        conn.close()

async def send_next_ui(update, context, user_id, quiz_id, prev_feedback="", reset_progress=False, use_callback=None):
    questions, grp_id, idx, grp_name = await get_question_data(user_id, quiz_id, reset=reset_progress)

    if questions is None:
        msg = update.callback_query.message if update.callback_query else update.message
        return await msg.reply_text("⚠️ هذا الاختبار لا يحتوي على ملفات أسئلة.")

    if idx >= len(questions):
        conn = get_db()
        try:
            next_grp = conn.execute('SELECT id, file_name FROM groups WHERE quiz_id=? AND id > ? ORDER BY id LIMIT 1', (quiz_id, grp_id)).fetchone()
        finally:
            conn.close()

        if next_grp:
            text = f"{prev_feedback}\n\n📦 **انتهت المجموعة الحالية.**\nماذا تريد أن تفعل؟" if prev_feedback else "📦 **انتهت المجموعة الحالية.**\nماذا تريد أن تفعل؟"
            keyboard = [
                [InlineKeyboardButton("❌ إنهاء الاختبار", callback_data=f"quit_{quiz_id}")],
                [InlineKeyboardButton(f"➡️ اكمال {next_grp[1]}", callback_data=f"continue_{quiz_id}_{next_grp[0]}")]
            ]
            if (use_callback is None and update.callback_query) or use_callback is True:
                await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id=user_id, text=text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return
        else:
            final = f"{prev_feedback}\n\n🎉 **تم الانتهاء من كافة أسئلة الاختبار!**"
            if (use_callback is None and update.callback_query) or use_callback is True:
                await update.callback_query.edit_message_text(final, parse_mode='Markdown')
            else:
                await context.bot.send_message(chat_id=user_id, text=final, parse_mode='Markdown')
            return

    q = questions[idx]
    total_questions = len(questions)
    header = f"📂 **المجموعة: {grp_name}**\n" if idx == 0 else ""
    full_text = f"{prev_feedback}\n\n{header}❓ **السؤال {idx+1}/{total_questions}:**\n{q[3]}"

    btns = []
    options = [('A', q[4]), ('B', q[5]), ('C', q[6]), ('D', q[7])]
    for letter, text in options:
        if text and isinstance(text, str) and text.strip() and text.strip().lower() != 'nan':
            btns.append([InlineKeyboardButton(f"{letter}) {text}", callback_data=f"ans_{letter}_{quiz_id}_{q[0]}")])
        elif text and not isinstance(text, str):
            str_text = str(text).strip()
            if str_text and str_text.lower() != 'nan':
                btns.append([InlineKeyboardButton(f"{letter}) {str_text}", callback_data=f"ans_{letter}_{quiz_id}_{q[0]}")])

    if (use_callback is None and update.callback_query) or use_callback is True:
        await update.callback_query.edit_message_text(full_text, reply_markup=InlineKeyboardMarkup(btns), parse_mode='Markdown')
    else:
        await context.bot.send_message(chat_id=user_id, text=full_text, reply_markup=InlineKeyboardMarkup(btns), parse_mode='Markdown')

# --- معالج الكول باك الجديد لتأكيد البريد ---
async def handle_broadcast_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "broadcast_yes":
        broadcast_text = context.user_data.get('broadcast_text')
        if not broadcast_text:
            await query.edit_message_text("❌ حدث خطأ: لم يتم العثور على نص الرسالة.")
            return

        await query.edit_message_text("⏳ جاري الإرسال... قد يستغرق هذا دقيقة.")

        conn = get_db()
        users = conn.execute('SELECT user_id FROM users').fetchall()
        total = len(users)
        success = 0
        died = 0

        for (uid,) in users:
            try:
                await context.bot.send_message(chat_id=uid, text=broadcast_text)
                success += 1
                conn.execute('UPDATE users SET fail_count = 0 WHERE user_id = ?', (uid,))
            except Exception:
                conn.execute('UPDATE users SET fail_count = fail_count + 1 WHERE user_id = ?', (uid,))
                fail = conn.execute('SELECT fail_count FROM users WHERE user_id = ?', (uid,)).fetchone()
                if fail and fail[0] >= 2:
                    died += 1

        conn.commit()
        conn.close()

        report = (
            f"📢 **تقرير الإرسال الجماعي:**\n\n"
            f"✅ تم الإرسال بنجاح لـ: `{success}` مستخدم\n"
            f"💀 مستخدمين ميتين (فشل مرتين متتاليتين): `{died}`\n"
            f"📊 إجمالي عدد المستخدمين في القاعدة: `{total}`"
        )
        await query.edit_message_text(report, parse_mode='Markdown')
        context.user_data.clear()

    elif data == "broadcast_no":
        await query.edit_message_text("❌ تم إلغاء الإرسال الجماعي.")
        context.user_data.clear()

# --- معالجة الأزرار (Callback Queries) الأصلية ---
async def handle_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    conn = get_db()
    user_id = query.from_user.id
    user = query.from_user

    try:
        if user_id != OWNER_ID:
            if not await is_bot_active_for_user(user_id):
                await query.answer("⛔ البوت متوقف حالياً.", show_alert=True)
                return

        if data.startswith('startquiz_'):
            quiz_id = int(data.split('_')[1])
            if not await check_subscription(user, context):
                conn.close()
                channel_link = get_setting('channel_link')
                show_link = get_setting('show_channel_link')
                keyboard = []
                if show_link == '1' and channel_link:
                    keyboard.append([InlineKeyboardButton("📢 اشترك في القناة", url=channel_link)])
                await query.edit_message_text(
                    "❌ عذراً، للوصول إلى هذا الاختبار يجب أن تكون مشتركاً في قناتنا أولاً.\n"
                    "يرجى الاشتراك ثم حاول مرة أخرى.",
                    reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
                )
                await query.answer()
                return
            conn.close()
            await send_next_ui(update, context, user_id, quiz_id, reset_progress=True, use_callback=True)

        elif data.startswith('ans_'):
            parts = data.split('_')
            choice = parts[1]
            quiz_id = int(parts[2])
            q_id = int(parts[3])
            q = conn.execute('SELECT stem, correct, explanation FROM questions WHERE id=?', (q_id,)).fetchone()
            conn.execute('UPDATE progress SET current_q_idx = current_q_idx + 1 WHERE user_id=? AND quiz_id=?',
                         (user_id, quiz_id))
            conn.commit()
            conn.close()
            icon = "✅" if choice == q[1] else "❌"
            feedback = (f"**السؤال السابق:** {q[0]}\n"
                        f"{icon} **إجابتك:** {choice} | **الصح:** {q[1]}\n"
                        f"💡 **الشرح:** {q[2]}")
            await send_next_ui(update, context, user_id, quiz_id, prev_feedback=feedback, use_callback=True)

        elif data.startswith('quit_'):
            quiz_id = int(data.split('_')[1])
            await query.message.edit_text("✅ **تم إنهاء الاختبار.** شكراً لمشاركتك!", parse_mode='Markdown')
            await query.answer()

        elif data.startswith('continue_'):
            try:
                parts = data.split('_')
                quiz_id = int(parts[1])
                next_grp_id = int(parts[2])
                if not await check_subscription(user, context):
                    conn.close()
                    channel_link = get_setting('channel_link')
                    show_link = get_setting('show_channel_link')
                    keyboard = []
                    if show_link == '1' and channel_link:
                        keyboard.append([InlineKeyboardButton("📢 اشترك في القناة", url=channel_link)])
                    await query.edit_message_text(
                        "❌ عذراً، للاستمرار في الاختبار يجب أن تكون مشتركاً في قناتنا.\n"
                        "يرجى الاشتراك ثم حاول مرة أخرى.",
                        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
                    )
                    await query.answer()
                    return
                conn.execute('UPDATE progress SET current_grp_id=?, current_q_idx=0 WHERE user_id=? AND quiz_id=?',
                             (next_grp_id, user_id, quiz_id))
                conn.commit()
                conn.close()
                await query.message.delete()
                await send_next_ui(update, context, user_id, quiz_id, use_callback=False)
                await query.answer()
            except Exception as e:
                logging.exception("خطأ في continue_")
                await query.answer(f"حدث خطأ: {str(e)}", show_alert=True)

        elif data.startswith('tog_'):
            quiz_id = int(data.split('_')[1])
            conn.execute('UPDATE quizzes SET is_active = 1 - is_active WHERE id=?', (quiz_id,))
            conn.commit()
            await query.answer("🔄 تم تحديث حالة الظهور")

        elif data.startswith('newpriv_'):
            try:
                quiz_id = int(data.split('_')[1])
                token = secrets.token_urlsafe(8)
                conn.execute('UPDATE quizzes SET private_token=? WHERE id=?', (token, quiz_id))
                conn.commit()
                bot_user = await context.bot.get_me()
                username = bot_user.username
                link = f"https://t.me/{username}?start={token}"
                await query.message.reply_text(f"🔗 رابط خاص جديد:\n`{link}`", parse_mode='Markdown')
                await query.answer("✅ تم توليد رابط جديد")
            except Exception as e:
                logging.exception("خطأ في معالجة newpriv")
                await query.answer(f"❌ حدث خطأ: {str(e)}", show_alert=True)

        elif data.startswith('setmax_'):
            quiz_id = int(data.split('_')[1])
            context.user_data['awaiting_max'] = quiz_id
            await query.message.reply_text("📝 أرسل العدد الأقصى للمستخدمين (0 يعني غير محدود):")
            await query.answer()

        elif data.startswith('showpriv_'):
            quiz_id = int(data.split('_')[1])
            users = conn.execute('''SELECT u.user_id, u.full_name, u.username, p.accessed_at
                                     FROM private_access p
                                     JOIN users u ON u.user_id = p.user_id
                                     WHERE p.quiz_id=?''', (quiz_id,)).fetchall()
            if not users:
                await query.message.reply_text("👥 لا يوجد مستخدمين خاصين حتى الآن.")
            else:
                msg = "📋 قائمة المستخدمين الخاصين:\n"
                for u in users:
                    msg += f"• {u[1]} (@{u[2]}) - {u[3]}\n"
                await query.message.reply_text(msg)
            await query.answer()

        elif data.startswith('clearpriv_'):
            quiz_id = int(data.split('_')[1])
            keyboard = [[
                InlineKeyboardButton("✅ نعم، احذف", callback_data=f"confirm_clear_{quiz_id}"),
                InlineKeyboardButton("❌ إلغاء", callback_data="cancel_clear")
            ]]
            await query.message.reply_text("⚠️ هل أنت متأكد من حذف جميع المستخدمين الخاصين لهذا الاختبار؟",
                                           reply_markup=InlineKeyboardMarkup(keyboard))
            await query.answer()

        elif data.startswith('confirm_clear_'):
            quiz_id = int(data.split('_')[2])
            conn.execute('DELETE FROM private_access WHERE quiz_id=?', (quiz_id,))
            conn.execute('UPDATE quizzes SET used_users=0 WHERE id=?', (quiz_id,))
            conn.commit()
            await query.message.edit_text("✅ تم مسح قائمة المستخدمين الخاصين.")
            await query.answer()

        elif data == 'cancel_clear':
            await query.message.delete()
            await query.answer()

        elif data.startswith('up_'):
            quiz_id = int(data.split('_')[1])
            context.user_data['up_id'] = quiz_id
            await query.message.reply_text("📥 أرسل ملف الإكسل الآن:")
            await query.answer()

        elif data.startswith('showf_'):
            quiz_id = int(data.split('_')[1])
            grps = conn.execute('SELECT id, file_name FROM groups WHERE quiz_id=?', (quiz_id,)).fetchall()
            for g in grps:
                btn = [[InlineKeyboardButton(f"🗑 حذف {g[1]}", callback_data=f"delgrp_{g[0]}")]]
                await context.bot.send_message(chat_id=OWNER_ID, text=f"📄 ملف: {g[1]}", reply_markup=InlineKeyboardMarkup(btn))
            await query.answer()

        elif data.startswith('delgrp_'):
            grp_id = int(data.split('_')[1])
            conn.execute('DELETE FROM questions WHERE group_id=?', (grp_id,))
            conn.execute('DELETE FROM groups WHERE id=?', (grp_id,))
            conn.commit()
            await query.message.delete()

        elif data.startswith('delquiz_'):
            quiz_id = int(data.split('_')[1])
            keyboard = [[
                InlineKeyboardButton("✅ نعم، احذف الاختبار", callback_data=f"confirm_delquiz_{quiz_id}"),
                InlineKeyboardButton("❌ إلغاء", callback_data="cancel_delquiz")
            ]]
            await query.message.reply_text("⚠️ هل أنت متأكد من حذف هذا الاختبار بالكامل؟\nسيتم حذف جميع المجموعات والأسئلة وتقدم المستخدمين والوصول الخاص.",
                                           reply_markup=InlineKeyboardMarkup(keyboard))
            await query.answer()

        elif data.startswith('confirm_delquiz_'):
            quiz_id = int(data.split('_')[2])
            conn.execute('DELETE FROM questions WHERE quiz_id=?', (quiz_id,))
            conn.execute('DELETE FROM groups WHERE quiz_id=?', (quiz_id,))
            conn.execute('DELETE FROM progress WHERE quiz_id=?', (quiz_id,))
            conn.execute('DELETE FROM private_access WHERE quiz_id=?', (quiz_id,))
            conn.execute('DELETE FROM quizzes WHERE id=?', (quiz_id,))
            conn.commit()
            await query.message.edit_text("✅ تم حذف الاختبار وجميع بياناته.")
            await query.answer()

        elif data == 'cancel_delquiz':
            await query.message.delete()
            await query.answer()

        elif data.startswith('editname_'):
            quiz_id = int(data.split('_')[1])
            context.user_data['awaiting_newname'] = quiz_id
            await query.message.reply_text("✏️ أرسل الاسم الجديد للاختبار:")
            await query.answer()

        elif data == 'set_channel_id':
            context.user_data['awaiting_channel_id'] = True
            await query.message.reply_text("📝 أرسل معرف القناة (مثال: @my_channel أو -1001234567890):")
            await query.answer()

        elif data == 'set_channel_link':
            context.user_data['awaiting_channel_link'] = True
            await query.message.reply_text("🔗 أرسل رابط القناة (مثال: https://t.me/my_channel):")
            await query.answer()

        elif data == 'clear_channel':
            update_setting('required_channel', '')
            update_setting('channel_link', '')
            await query.message.edit_text("✅ تم إلغاء فرض الاشتراك في القناة.")
            await query.answer()

        elif data == 'toggle_show_link':
            current = get_setting('show_channel_link')
            new_value = '0' if current == '1' else '1'
            update_setting('show_channel_link', new_value)
            status = "مفعل ✅" if new_value == '1' else "معطل ❌"
            await query.message.edit_text(
                f"🔗 تم تغيير حالة إظهار رابط القناة إلى: {status}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("رجوع", callback_data="back_to_channel_settings")
                ]])
            )
            await query.answer()

        elif data == 'back_to_channel_settings':
            current_channel = get_setting('required_channel')
            current_link = get_setting('channel_link')
            show_link = get_setting('show_channel_link')
            channel_display = current_channel if current_channel else 'غير محدد'
            link_display = current_link if current_link else 'غير محدد'
            show_status = "مفعل ✅" if show_link == '1' else "معطل ❌"

            settings_text = (
                f"🔧 **إعدادات القناة الإجبارية:**\n"
                f"• معرف القناة: {channel_display}\n"
                f"• رابط القناة: {link_display}\n"
                f"• إظهار الرابط للمستخدمين: {show_status}\n"
            )

            settings_buttons = [
                [InlineKeyboardButton("✏️ تغيير معرف القناة", callback_data="set_channel_id")],
                [InlineKeyboardButton("🔗 تغيير رابط القناة", callback_data="set_channel_link")],
                [InlineKeyboardButton("🗑️ إلغاء فرض القناة", callback_data="clear_channel")],
                [InlineKeyboardButton(f"👁️ إظهار الرابط: {show_status}", callback_data="toggle_show_link")]
            ]
            await query.message.edit_text(
                settings_text,
                reply_markup=InlineKeyboardMarkup(settings_buttons),
                parse_mode='Markdown'
            )
            await query.answer()

        elif data == 'toggle_bot':
            current = get_setting('bot_active')
            new_value = '0' if current == '1' else '1'
            update_setting('bot_active', new_value)
            status_text = "نشط ✅" if new_value == '1' else "متوقف ⛔"
            await query.message.edit_text(
                f"⚡ تم تغيير حالة البوت إلى: {status_text}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("رجوع", callback_data="back_to_bot_settings")
                ]])
            )
            await query.answer()

        elif data == 'back_to_bot_settings':
            current = get_setting('bot_active')
            status_text = "نشط ✅" if current == '1' else "متوقف ⛔"
            text = f"⚡ **حالة البوت الحالية:** {status_text}\n\nاختر الإجراء المطلوب:"
            keyboard = [[InlineKeyboardButton("🔁 تبديل الحالة", callback_data="toggle_bot")]]
            await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            await query.answer()

    finally:
        try:
            conn.close()
        except:
            pass

# --- دالة مسح سجلات التقدم ---
async def clear_progress_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        conn = sqlite3.connect('quiz_system.db')
        cursor = conn.cursor()
        cursor.execute('DELETE FROM progress')
        conn.commit()
        conn.close()
        await update.message.reply_text("✅ تم مسح جميع سجلات تقدم المستخدمين بنجاح.")
    except Exception as e:
        await update.message.reply_text(f"❌ حدث خطأ أثناء المسح: {e}")

# --- لوحة الإدارة ---
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return

    keyboard = [
        ["➕ إنشاء اختبار", "⚙️ إدارة الاختبارات"],
        ["🔧 إعدادات القناة", "⚡ تشغيل/إيقاف البوت"],
        ["🧹 تصفير السجلات", "📧 البريد"]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "🛠 **أهلاً بك في لوحة التحكم:**\nإختر أحد الخيارات من القائمة أدناه:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

# --- معالجة النصوص من المشرف ---
async def handle_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text
    conn = get_db()

    if txt == "📧 البريد":
        await update.message.reply_text("📝 أرسل الآن نص الرسالة التي تريد إرسالها لجميع المستخدمين.")
        context.user_data['awaiting_broadcast_text'] = True
        return

    if context.user_data.get('awaiting_broadcast_text'):
        # تخزين النص وعرض التأكيد
        broadcast_text = txt
        context.user_data['broadcast_text'] = broadcast_text
        del context.user_data['awaiting_broadcast_text']

        keyboard = [
            [InlineKeyboardButton("✅ تأكيد الإرسال", callback_data="broadcast_yes")],
            [InlineKeyboardButton("❌ إلغاء", callback_data="broadcast_no")]
        ]
        await update.message.reply_text(
            f"📋 **نص الرسالة:**\n\n{broadcast_text}\n\nهل أنت متأكد من إرسالها لجميع المستخدمين؟",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return

    if txt == "🧹 تصفير السجلات":
        await clear_progress_data(update, context)
        return

    try:
        if context.user_data.get('awaiting_channel_id'):
            update_setting('required_channel', txt)
            del context.user_data['awaiting_channel_id']
            await update.message.reply_text(f"✅ تم تعيين معرف القناة إلى: {txt}")
            return

        if context.user_data.get('awaiting_channel_link'):
            update_setting('channel_link', txt)
            del context.user_data['awaiting_channel_link']
            await update.message.reply_text(f"✅ تم تعيين رابط القناة إلى: {txt}")
            return

        if 'awaiting_newname' in context.user_data:
            quiz_id = context.user_data['awaiting_newname']
            try:
                conn.execute('UPDATE quizzes SET name=? WHERE id=?', (txt, quiz_id))
                conn.commit()
                await update.message.reply_text(f"✅ تم تحديث اسم الاختبار إلى: {txt}")
            except Exception as e:
                await update.message.reply_text(f"❌ حدث خطأ أثناء تحديث الاسم: {e}")
            finally:
                del context.user_data['awaiting_newname']
            return

        if txt == "➕ إنشاء اختبار":
            await update.message.reply_text("أرسل اسم الاختبار:")
            context.user_data['state'] = 'naming'

        elif context.user_data.get('state') == 'naming':
            conn.execute('INSERT INTO quizzes (name) VALUES (?)', (txt,))
            conn.commit()
            await update.message.reply_text(f"✅ تم إنشاء الاختبار: {txt}")
            context.user_data['state'] = None

        elif txt == "⚙️ إدارة الاختبارات":
            quizzes = conn.execute('''
                SELECT 
                    q.id, 
                    q.name, 
                    q.is_active,
                    q.max_users,
                    q.used_users,
                    (SELECT COUNT(*) FROM groups WHERE quiz_id = q.id) as files_count,
                    (SELECT COUNT(*) FROM questions WHERE quiz_id = q.id) as questions_count,
                    (SELECT COUNT(DISTINCT user_id) FROM progress WHERE quiz_id = q.id) as users_count
                FROM quizzes q
            ''').fetchall()

            if not quizzes:
                await update.message.reply_text("📭 لا توجد اختبارات مضافة بعد.")
            else:
                for q in quizzes:
                    qid, name, active, maxu, used, files_count, questions_count, users_count = q
                    status = "🟢 نشط" if active else "🔴 مخفي"
                    priv_info = f"👥 {used}/{maxu if maxu>0 else '∞'}"
                    info_text = (f"📑 **{name}**\n"
                                 f"📂 الملفات: {files_count} | ❓ الأسئلة: {questions_count} | 👥 المستخدمين: {users_count}\n"
                                 f"الحالة: {status} | الحد الأقصى: {priv_info}")

                    btns = [
                        [InlineKeyboardButton("➕ رفع ملف", callback_data=f"up_{qid}"),
                         InlineKeyboardButton("📂 الملفات", callback_data=f"showf_{qid}")],
                        [InlineKeyboardButton(f"الحالة: {status}", callback_data=f"tog_{qid}"),
                         InlineKeyboardButton("🔗 رابط خاص جديد", callback_data=f"newpriv_{qid}")],
                        [InlineKeyboardButton(f"⚙️ حد أقصى {priv_info}", callback_data=f"setmax_{qid}"),
                         InlineKeyboardButton("👥 عرض المستخدمين", callback_data=f"showpriv_{qid}")],
                        [InlineKeyboardButton("🗑 مسح القائمة الخاصة", callback_data=f"clearpriv_{qid}"),
                         InlineKeyboardButton("❌ حذف الاختبار", callback_data=f"delquiz_{qid}"),
                         InlineKeyboardButton("✏️ تعديل الاسم", callback_data=f"editname_{qid}")]
                    ]
                    await update.message.reply_text(info_text, reply_markup=InlineKeyboardMarkup(btns), parse_mode='Markdown')

        elif txt == "🔧 إعدادات القناة":
            current_channel = get_setting('required_channel')
            current_link = get_setting('channel_link')
            show_link = get_setting('show_channel_link')
            channel_display = current_channel if current_channel else 'غير محدد'
            link_display = current_link if current_link else 'غير محدد'
            show_status = "مفعل ✅" if show_link == '1' else "معطل ❌"

            settings_text = (
                f"🔧 **إعدادات القناة الإجبارية:**\n"
                f"• معرف القناة: {channel_display}\n"
                f"• رابط القناة: {link_display}\n"
                f"• إظهار الرابط للمستخدمين: {show_status}\n"
            )

            settings_buttons = [
                [InlineKeyboardButton("✏️ تغيير معرف القناة", callback_data="set_channel_id")],
                [InlineKeyboardButton("🔗 تغيير رابط القناة", callback_data="set_channel_link")],
                [InlineKeyboardButton("🗑️ إلغاء فرض القناة", callback_data="clear_channel")],
                [InlineKeyboardButton(f"👁️ إظهار الرابط: {show_status}", callback_data="toggle_show_link")]
            ]

            await update.message.reply_text(
                settings_text,
                reply_markup=InlineKeyboardMarkup(settings_buttons),
                parse_mode='Markdown'
            )

        elif txt == "⚡ تشغيل/إيقاف البوت":
            current = get_setting('bot_active')
            status_text = "نشط ✅" if current == '1' else "متوقف ⛔"
            text = f"⚡ **حالة البوت الحالية:** {status_text}\n\nاختر الإجراء المطلوب:"
            keyboard = [[InlineKeyboardButton("🔁 تبديل الحالة", callback_data="toggle_bot")]]
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

        elif 'awaiting_max' in context.user_data:
            try:
                new_max = int(txt)
                quiz_id = context.user_data['awaiting_max']
                conn.execute('UPDATE quizzes SET max_users=? WHERE id=?', (new_max, quiz_id))
                conn.commit()
                await update.message.reply_text(f"✅ تم تعيين الحد الأقصى للاختبار إلى {new_max}.")
            except ValueError:
                await update.message.reply_text("❌ الرجاء إدخال رقم صحيح.")
            finally:
                del context.user_data['awaiting_max']

    finally:
        conn.close()

# --- رفع ملف إكسل ---
async def on_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID or not context.user_data.get('up_id'):
        return
    qid = context.user_data['up_id']
    doc = update.message.document
    file = await doc.get_file()
    file_bytes = await file.download_as_bytearray()
    df = pd.read_excel(io.BytesIO(file_bytes))

    conn = get_db()
    try:
        cur = conn.cursor()
        group_name = os.path.splitext(doc.file_name)[0]
        cur.execute('INSERT INTO groups (quiz_id, file_name) VALUES (?,?)', (qid, group_name))
        grp_id = cur.lastrowid
        for _, r in df.iterrows():
            stem = str(r.get('Question_Stem', ''))
            a = str(r.get('answer_A', ''))
            b = str(r.get('answer_B', ''))
            c = str(r.get('answer_C', ''))
            d = str(r.get('answer_D', ''))
            correct = str(r.get('Correct_Answer', '')).strip().upper()
            explanation = str(r.get('Explanation', 'لا يوجد شرح'))
            conn.execute('''INSERT INTO questions 
                (quiz_id, group_id, stem, a, b, c, d, correct, explanation) 
                VALUES (?,?,?,?,?,?,?,?,?)''',
                (qid, grp_id, stem, a, b, c, d, correct, explanation))
        conn.commit()
        await update.message.reply_text(f"✅ تم استيراد '{doc.file_name}' بنجاح.")
    except Exception as e:
        await update.message.reply_text(f"❌ حدث خطأ أثناء استيراد الملف: {e}")
    finally:
        conn.close()

# --- التشغيل الرئيسي ---
def main():
    init_db()
    keep_alive()

    while True:
        try:
            logger.info("يتم الآن تجهيز اتصال البوت...")
            app_tg = Application.builder().token(BOT_TOKEN).build()

            app_tg.add_handler(CommandHandler("start", start))
            app_tg.add_handler(CommandHandler("admin", admin_panel))
            app_tg.add_handler(MessageHandler(filters.Regex("^(➕ إنشاء اختبار|⚙️ إدارة الاختبارات|🔧 إعدادات القناة|⚡ تشغيل/إيقاف البوت|🧹 تصفير السجلات|📧 البريد)$"), handle_admin_text))
            app_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_text))
            app_tg.add_handler(MessageHandler(filters.Document.ALL, on_file_upload))
            app_tg.add_handler(CallbackQueryHandler(handle_broadcast_confirmation, pattern="^broadcast_"))
            app_tg.add_handler(CallbackQueryHandler(handle_callbacks))
            logger.info("البوت بدأ العمل بنجاح...")
            app_tg.run_polling(drop_pending_updates=True)

        except Exception as e:
            logger.error(f"حدث خطأ غير متوقع: {e}")
            logger.info("سيتم إعادة تشغيل البوت خلال 10 ثوانٍ...")
            time.sleep(10)

if __name__ == '__main__':
    main()