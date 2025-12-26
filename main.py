from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, ContextTypes, filters
from PIL import Image
import os
BOT_TOKEN = "8525502008:AAGSOHhtH89KJ2sGNRxEiUd0RGA6RkAhPNA"
user_images = {}
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "أرسل الصور، ثم اكتب /pdf لتحويلها إلى ملف واحد.")
async def handle_image(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    photo = update.message.photo[-1]
    file = await photo.get_file()
    os.makedirs(f"temp/{user_id}", exist_ok=True)
    path = f"temp/{user_id}/{photo.file_unique_id}.jpg"
    await file.download_to_drive(path)
    user_images.setdefault(user_id, []).append(path)
    await update.message.reply_text("تم استلام الصورة ✅")
async def make_pdf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    images = user_images.get(user_id)
    if not images:
        await update.message.reply_text("لم ترسل أي صور.")
        return
    pil_images = [Image.open(img).convert("RGB") for img in images]
    pdf_path = f"temp/{user_id}/result.pdf"
    pil_images[0].save(pdf_path, save_all=True, append_images=pil_images[1:])
    await update.message.reply_document(open(pdf_path, "rb"))
    # تنظيف
    for img in images:
        os.remove(img)
    os.remove(pdf_path)
    user_images[user_id] = []
app = ApplicationBuilder().token(BOT_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("pdf", make_pdf))
app.add_handler(MessageHandler(filters.PHOTO, handle_image))
app.run_polling()
