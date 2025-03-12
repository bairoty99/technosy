import os
import asyncio
import re
import logging
import shlex
from telethon import TelegramClient, events, Button
import yt_dlp
import instaloader
import ffmpeg
from dotenv import load_dotenv
import shutil
import aiosqlite
from slugify import slugify
from pathlib import Path
import aiofiles
from asyncio import Queue, Semaphore
from concurrent.futures import ProcessPoolExecutor
import time
from telegraph import Telegraph
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import validators
from urllib.parse import urlparse
import subprocess

# إعداد التسجيل
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# تحميل الإعدادات من .env
load_dotenv()
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
bot_token = os.getenv('BOT_TOKEN')
DEVELOPER_ID = int(os.getenv('DEVELOPER_ID', '0'))
GOOGLE_CREDS = os.getenv('GOOGLE_CREDS_JSON')
COOKIES_PATH = '/root/grokthunder-bot/youtube_cookies.txt'

# التحقق من الإعدادات الأساسية
if not all([api_id, api_hash, bot_token]):
    logging.error("Missing API credentials! Check your .env file.")
    exit(1)

# إعداد العميل
client = TelegramClient('TechnoSyriaBot', api_id, api_hash)

# أنماط الروابط المحسنة
YT_PATTERN = r'https?://(?:www\.)?(youtube\.com|youtu\.be)/[^\s]+'
INSTA_REELS_PATTERN = r'https?://(?:www\.)?instagram\.com/reel/([^/\s?]+)'
INSTA_PATTERN = r'https?://(?:www\.)?instagram\.com/(?:p|stories|tv)/([^/\s?]+)'
TIKTOK_PATTERN = r'https?://(?:www\.)?(tiktok\.com|vm\.tiktok\.com)/[^\s]+'
FB_PATTERN = r'https?://(?:www\.)?(facebook\.com|fb\.watch)/[^\s]+'
TWITTER_PATTERN = r'https?://(?:www\.)?(twitter\.com|x\.com)/[^\s]+'
TELEGRAM_STORY_PATTERN = r'https?://t\.me/[^/]+/s/(\d+)'

# المتغيرات العامة
download_queue = Queue()
stats = {'downloads': 0, 'errors': 0}
banned_users = set()
muted_users = set()
semaphore = Semaphore(3)
active_downloads = {}
telegraph = Telegraph()
telegraph.create_account(short_name='TechnoSyriaBot')

# Google Drive مع التحقق
drive_service = None
if GOOGLE_CREDS and os.path.exists(GOOGLE_CREDS):
    try:
        creds = Credentials.from_authorized_user_file(GOOGLE_CREDS)
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.warning(f"Google Drive setup failed: {str(e)}")

# قاعدة البيانات
async def get_db():
    try:
        db = await aiosqlite.connect('cache.db')
        yield db
    except aiosqlite.Error as e:
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        await db.close()

async def init_db(db):
    await db.execute('''CREATE TABLE IF NOT EXISTS cache (url TEXT PRIMARY KEY, file_path TEXT, timestamp REAL)''')
    await db.commit()

# تنظيف الملفات
async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)
        total_size = sum(os.path.getsize(f) for f in Path("downloads").glob("**/*") if f.is_file()) / (1024 * 1024)
        if total_size > 500:
            async for db in get_db():
                async with db.execute("SELECT file_path, timestamp FROM cache WHERE timestamp < ?", 
                                     (time.time() - 24*3600,)) as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    file_path, _ = row
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    await db.execute("DELETE FROM cache WHERE file_path = ?", (file_path,))
                    await db.commit()
            logging.info(f"Cleaned up {total_size:.2f}MB.")

# التحقق من المتطلبات
def check_ffmpeg():
    return shutil.which("ffmpeg") is not None

def validate_url(url):
    return validators.url(url) and bool(urlparse(url).scheme)

def check_cookies():
    return os.path.exists(COOKIES_PATH)

# أمر /start
@client.on(events.NewMessage(pattern='/start'))
async def start_command(event):
    welcome_msg = (
        "🌌 **@techno_syria_bot - القوة المطلقة!** 🌌\n"
        "🔗 أرسل رابط Reels لتحميله فوراً.\n"
        "📁 أرسل فيديو/ملف لخيارات الضغط والتحويل.\n\n"
        "🔹 `/yt [اسم]` - ابحث في يوتيوب\n"
        "🔹 `/help` - تعليمات سريعة\n"
        "🔹 `/stats` - إحصائيات البوت"
    )
    buttons = [
        [Button.inline("📹 يوتيوب", "yt_help"), Button.inline("📸 إنستغرام", "insta_help")],
        [Button.inline("⚙️ أدوات", "tools_help"), Button.inline("ℹ️ الحالة", "status")]
    ]
    await event.reply(welcome_msg, buttons=buttons, parse_mode='markdown')

# أوامر إضافية
@client.on(events.NewMessage(pattern='/help'))
async def help_command(event):
    msg = (
        "📚 **دليل @techno_syria_bot**\n"
        "🔗 **رابط Reels**: تحميل فوري بضغطة واحدة.\n"
        "📁 **فيديو/ملف**: خيارات ضغط، تحويل، رفع.\n"
        "🔹 `/yt [اسم]` - بحث ذكي في يوتيوب.\n"
        "🔹 `/cancel` - إيقاف أي عملية."
    )
    await event.reply(msg, parse_mode='markdown')

@client.on(events.NewMessage(pattern='/stats'))
async def stats_command(event):
    total_size = sum(os.path.getsize(f) for f in Path("downloads").glob("**/*") if f.is_file()) / (1024 * 1024)
    msg = f"📊 **إحصائيات البوت**\n🔹 تحميلات: {stats['downloads']}\n🔹 أخطاء: {stats['errors']}\n🔹 حجم المؤقت: {total_size:.2f}MB"
    await event.reply(msg, parse_mode='markdown')

@client.on(events.NewMessage(pattern='/cancel'))
async def cancel_command(event):
    if event.sender_id in active_downloads:
        active_downloads[event.sender_id].cancel()
        del active_downloads[event.sender_id]
        await event.reply("🛑 **تم الإلغاء بنجاح!**\n@techno_syria_bot", parse_mode='markdown')
    else:
        await event.reply("❌ **لا توجد عمليات نشطة!**\n@techno_syria_bot", parse_mode='markdown')

# مساعدة وحالة
@client.on(events.CallbackQuery(pattern=r'(yt|insta|tools|status)_help'))
async def help_handler(event):
    platform = event.data.decode().split('_')[0]
    messages = {
        'yt': "📹 **يوتيوب**: تحميل فيديوهات بجودات متعددة (حتى 4K) مع خيارات تحويل!",
        'insta': "📸 **إنستغرام**: Reels تُحمل فوراً، بقية المنشورات بسرعة خارقة!",
        'tools': "⚙️ **أدوات البوت**:\n- **ضغط**: تصغير حجم الفيديو.\n- **MP3**: استخراج الصوت.\n- **GIF**: تحويل إلى صورة متحركة.\n- **Drive/Telegraph**: رفع الملفات!",
        'status': f"ℹ️ **حالة البوت**\nFFmpeg: {'✅' if check_ffmpeg() else '❌'}\nDrive: {'✅' if drive_service else '❌'}\nCookies: {'✅' if check_cookies() else '❌'}\n@techno_syria_bot"
    }
    await event.reply(messages[platform], parse_mode='markdown')

# تقسيم الملفات
async def split_file(file_path, chunk_size=50*1024*1024):
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    if file_size > 2000:
        raise ValueError("الملف أكبر من 2GB!")
    async with aiofiles.open(file_path, 'rb') as f:
        content = await f.read()
    parts = []
    for i in range(0, len(content), chunk_size):
        part_path = f"{file_path}.part{i//chunk_size}"
        async with aiofiles.open(part_path, 'wb') as part_file:
            await part_file.write(content[i:i + chunk_size])
        parts.append(part_path)
    return parts

# تشغيل FFmpeg
async def run_ffmpeg(cmd, timeout=300):
    if not check_ffmpeg():
        raise RuntimeError("FFmpeg غير مثبت!")
    args = shlex.split(cmd)
    try:
        process = await asyncio.create_subprocess_exec(*args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg فشل: {stderr.decode().strip()}")
        return True, ""
    except asyncio.TimeoutError:
        process.kill()
        raise RuntimeError(f"FFmpeg تجاوز الوقت ({timeout} ثانية)!")
    except Exception as e:
        raise RuntimeError(str(e))

# ضغط الفيديو
async def compress_video(input_path, output_path, max_size_mb=50):
    size = os.path.getsize(input_path) / (1024 * 1024)
    if size <= max_size_mb:
        shutil.copy(input_path, output_path)
        return True, ""
    cmd = f"ffmpeg -i {input_path} -vcodec libx264 -crf 23 -preset medium -acodec aac -b:a 128k {output_path} -y"
    return await run_ffmpeg(cmd)

# تحويل إلى GIF
async def convert_to_gif(input_path, output_path, fps=15):
    cmd = f"ffmpeg -i {input_path} -vf 'fps={fps},scale=320:-1' -loop 0 {output_path} -y"
    return await run_ffmpeg(cmd)

# تحويل إلى MP3
async def convert_to_mp3(input_path, output_path):
    cmd = f"ffmpeg -i {input_path} -vn -acodec mp3 -ab 192k {output_path} -y"
    return await run_ffmpeg(cmd)

# إرسال الملف
async def send_file(chat, file, as_doc=False, caption="", retries=3):
    for attempt in range(retries):
        try:
            await client.send_file(chat, file, force_document=as_doc, caption=caption, parse_mode='markdown')
            return True
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(5)
                continue
            logging.error(f"Failed to send file: {str(e)}")
            return False

# رفع إلى Drive/Telegraph
async def upload_to_drive(file_path):
    if not drive_service:
        raise RuntimeError("Drive غير مفعل!")
    media = MediaFileUpload(file_path)
    file = drive_service.files().create(body={'name': os.path.basename(file_path)}, media_body=media, fields='webViewLink').execute()
    return file.get('webViewLink')

async def upload_to_telegraph(file_path):
    with open(file_path, 'rb') as f:
        response = telegraph.upload_file(f)
        if not response:
            raise RuntimeError("فشل رفع الملف إلى Telegraph!")
        return f"https://telegra.ph{response[0]['src']}"

# إعادة المحاولة
async def retry_on_failure(func, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return await func()
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                continue
            raise e

# تحميل الوسائط
async def download_media(url, event, platform, quality='best', audio_only=False, as_doc=False, to_gif=False, share_link=False, to_drive=False, is_playlist=False):
    if event.sender_id in banned_users:
        await event.reply("❌ **أنت محظور!**\n@techno_syria_bot")
        return
    if not validate_url(url):
        await event.reply("❌ **رابط غير صالح!**\n@techno_syria_bot")
        return

    # التحقق المسبق
    if not check_cookies() and platform.lower() == 'youtube':
        await event.reply("⚠️ **تحذير:** ملف الكوكيز مفقود، قد يفشل تحميل يوتيوب!\n@techno_syria_bot")
    if to_gif or not audio_only:
        if not check_ffmpeg():
            await event.reply("❌ **خطأ:** FFmpeg غير مثبت!\n@techno_syria_bot")
            return

    async with semaphore:
        task = asyncio.create_task(process_download(url, event, platform, quality, audio_only, as_doc, to_gif, share_link, to_drive, is_playlist))
        active_downloads[event.sender_id] = task
        try:
            await task
        except asyncio.CancelledError:
            await event.reply("🛑 **تم الإلغاء!**\n@techno_syria_bot")
        except Exception as e:
            await event.reply(f"❌ **خطأ:** {str(e)}\n@techno_syria_bot")
        finally:
            if event.sender_id in active_downloads:
                del active_downloads[event.sender_id]

async def process_download(url, event, platform, quality, audio_only, as_doc, to_gif, share_link, to_drive, is_playlist):
    status_msg = await event.reply(f"⚡ **جاري تحميل {platform}...** ⏳", parse_mode='markdown')
    files = []
    async for db in get_db():
        async with db.execute("SELECT file_path FROM cache WHERE url=?", (url,)) as cursor:
            cached = await cursor.fetchone()
        if cached and os.path.exists(cached[0]) and os.path.getsize(cached[0]) > 0 and not is_playlist:
            files = [cached[0]]
            await status_msg.edit("⚡ **تم العثور على الملف في المخبأ!** ⏳")
        else:
            ydl_opts = {
                'format': 'bestvideo[height<=720]+bestaudio/best[height<=720]' if quality == 'best' else quality,
                'outtmpl': 'downloads/%(title)s.%(ext)s',
                'quiet': True,
                'merge_output_format': 'mp4',
                'max_filesize': 2 * 1024 * 1024 * 1024,
                'noplaylist': not is_playlist,
                'cookiefile': COOKIES_PATH,
                'http_headers': {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124'},
            }
            if audio_only:
                ydl_opts['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
                del ydl_opts['merge_output_format']
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = await retry_on_failure(lambda: ydl.extract_info(url, download=True))
                    if not info or not info.get('title'):
                        raise ValueError("فشل استخراج المعلومات!")
                    if is_playlist:
                        files = [f"downloads/{slugify(entry['title'])}.{'mp3' if audio_only else 'mp4'}" for entry in info['entries'] if entry]
                    else:
                        file_path = f"downloads/{slugify(info['title'])}.{'mp3' if audio_only else 'mp4'}"
                        files = [file_path]
                processed_files = []
                for file in files:
                    if not os.path.exists(file) or os.path.getsize(file) == 0:
                        raise FileNotFoundError(f"الملف {file} غير موجود!")
                    if not audio_only and not to_gif:
                        output = f"{os.path.splitext(file)[0]}_compressed.mp4"
                        success, _ = await compress_video(file, output)
                        if not success:
                            raise RuntimeError("فشل الضغط!")
                        os.remove(file)
                        processed_files.append(output)
                    elif to_gif:
                        output = f"{os.path.splitext(file)[0]}.gif"
                        success, _ = await convert_to_gif(file, output)
                        if not success:
                            raise RuntimeError("فشل تحويل GIF!")
                        os.remove(file)
                        processed_files.append(output)
                    else:
                        processed_files.append(file)
                files = processed_files
                if not is_playlist and files:
                    await db.execute("INSERT OR REPLACE INTO cache (url, file_path, timestamp) VALUES (?, ?, ?)", 
                                    (url, files[0], time.time()))
                    await db.commit()
            except Exception as e:
                stats['errors'] += 1
                await status_msg.edit(f"❌ **فشل التحميل:** {str(e)}\n@techno_syria_bot", 
                                     buttons=[Button.inline("🔄 حاول مجدداً", f"retry_{platform}_{url}")])
                return
    try:
        await status_msg.edit("⚡ **جاري الإرسال...** ⏳")
        for file in files:
            size = os.path.getsize(file) / (1024 * 1024)
            caption = f"{'🎵' if audio_only else '🎬' if to_gif else '🎥'} **{os.path.basename(file)}**\n@techno_syria_bot"
            if share_link:
                link = await upload_to_telegraph(file)
                await event.reply(f"🔗 **رابط Telegraph:** {link}\n@techno_syria_bot")
            elif to_drive:
                link = await upload_to_drive(file)
                await event.reply(f"📂 **رابط Drive:** {link}\n@techno_syria_bot")
            elif size > 50:
                parts = await split_file(file)
                for i, part in enumerate(parts, 1):
                    await send_file(event.chat_id, part, as_doc, f"📦 **جزء {i}/{len(parts)}**\n{caption}")
                    os.remove(part)
            else:
                await send_file(event.chat_id, file, as_doc, caption)
            stats['downloads'] += 1
            if not cached or is_playlist:
                os.remove(file)
        await status_msg.delete()
    except Exception as e:
        stats['errors'] += 1
        await status_msg.edit(f"❌ **فشل الإرسال:** {str(e)}\n@techno_syria_bot")

# تحميل Reels فوراً
async def download_instagram_reels(url, event):
    if not validate_url(url):
        await event.reply("❌ **رابط غير صالح!**\n@techno_syria_bot")
        return
    async with semaphore:
        task = asyncio.create_task(process_instagram_reels(url, event))
        active_downloads[event.sender_id] = task
        try:
            await task
        except Exception:
            if event.sender_id in active_downloads:
                del active_downloads[event.sender_id]

async def process_instagram_reels(url, event):
    status_msg = await event.reply("⚡ **جاري تحميل Reel...** ⏳", parse_mode='markdown')
    match = re.search(INSTA_REELS_PATTERN, url)
    if not match:
        stats['errors'] += 1
        await status_msg.edit("❌ **رابط Reel غير صالح!**\n@techno_syria_bot")
        return
    shortcode = match.group(1)
    file_path = None
    try:
        L = instaloader.Instaloader(dirname_pattern="downloads/{shortcode}", download_comments=False, save_metadata=False)
        post = await asyncio.get_running_loop().run_in_executor(None, lambda: instaloader.Post.from_shortcode(L.context, shortcode))
        if not post.is_video or not post.video_url:
            raise ValueError("المحتوى ليس Reel أو خاص!")
        await asyncio.get_running_loop().run_in_executor(None, lambda: L.download_post(post, "downloads"))
        file_path = f"downloads/{shortcode}/{shortcode}.mp4"
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            raise FileNotFoundError("فشل تحميل Reel!")
        caption = f"🎥 **Reel: {post.caption[:50] + '...' if post.caption else 'بدون عنوان'}**\n@techno_syria_bot"
        await status_msg.edit("⚡ **جاري إرسال Reel...** ⏳")
        if os.path.getsize(file_path) / (1024 * 1024) > 50:
            parts = await split_file(file_path)
            for i, part in enumerate(parts, 1):
                await send_file(event.chat_id, part, False, f"📦 **جزء {i}/{len(parts)}**\n{caption}")
                os.remove(part)
        else:
            await send_file(event.chat_id, file_path, False, caption)
        stats['downloads'] += 1
        await status_msg.delete()
    except Exception as e:
        stats['errors'] += 1
        await status_msg.edit(f"❌ **فشل تحميل Reel:** {str(e)}\n@techno_syria_bot", 
                             buttons=[Button.inline("🔄 حاول مجدداً", f"retry_reels_{url}")])
    finally:
        if file_path and os.path.exists(os.path.dirname(file_path)):
            shutil.rmtree(os.path.dirname(file_path))

# معالجة الملفات المرفوعة
@client.on(events.NewMessage(incoming=True))
async def handle_message(event):
    text = event.raw_text
    if event.message.media and hasattr(event.message.media, 'document'):
        file = event.message.media.document
        mime_type = file.mime_type
        if mime_type.startswith('video/') or mime_type.startswith('audio/'):
            file_path = await client.download_media(event.message, "downloads/uploaded_file")
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                await event.reply("❌ **فشل رفع الملف!**\n@techno_syria_bot")
                return
            buttons = [
                [Button.inline("📥 ضغط", f"compress_{file_path}"), Button.inline("🎵 MP3", f"mp3_{file_path}")],
                [Button.inline("🎬 GIF", f"gif_{file_path}"), Button.inline("📂 Drive", f"drive_{file_path}")],
                [Button.inline("🔗 Telegraph", f"telegraph_{file_path}")]
            ]
            await event.reply(
                "🎥 **اختر خياراً لمعالجة الملف:**\n"
                "- **ضغط**: تقليل الحجم لتوفير المساحة.\n"
                "- **MP3**: استخراج الصوت فقط.\n"
                "- **GIF**: تحويل إلى صورة متحركة.\n"
                "- **Drive**: رفع إلى Google Drive.\n"
                "- **Telegraph**: رابط سريع للمشاركة.\n@techno_syria_bot",
                buttons=buttons, parse_mode='markdown'
            )
    elif re.search(INSTA_REELS_PATTERN, text):
        await download_instagram_reels(text, event)
    elif re.search(YT_PATTERN, text):
        is_playlist = 'playlist' in text.lower() or 'list=' in text
        await download_media(text, event, 'YouTube', is_playlist=is_playlist)
    elif re.search(INSTA_PATTERN, text):
        await download_media(text, event, 'Instagram')
    elif re.search(TIKTOK_PATTERN, text):
        await download_media(text, event, 'TikTok')
    elif re.search(FB_PATTERN, text):
        await download_media(text, event, 'Facebook')
    elif re.search(TWITTER_PATTERN, text):
        await download_media(text, event, 'Twitter')
    elif re.search(TELEGRAM_STORY_PATTERN, text):
        await download_media(text, event, 'Telegram')

# معالجة خيارات الملفات
@client.on(events.CallbackQuery(pattern=r'(compress|mp3|gif|drive|telegraph)_.+'))
async def process_file_options(event):
    data = event.data.decode().split('_', 1)
    action, file_path = data[0], data[1]
    if not os.path.exists(file_path):
        await event.reply("❌ **الملف مفقود!**\n@techno_syria_bot")
        return
    status_msg = await event.reply(f"⚡ **جاري معالجة الملف ({action})...** ⏳", parse_mode='markdown')
    try:
        if action == 'compress':
            output = f"{os.path.splitext(file_path)[0]}_compressed.mp4"
            success, _ = await compress_video(file_path, output)
            if not success:
                raise RuntimeError("فشل الضغط!")
            caption = f"🎥 **فيديو مضغوط**\n@techno_syria_bot"
            await send_file(event.chat_id, output, False, caption)
            os.remove(output)
        elif action == 'mp3':
            output = f"{os.path.splitext(file_path)[0]}.mp3"
            success, _ = await convert_to_mp3(file_path, output)
            if not success:
                raise RuntimeError("فشل تحويل MP3!")
            caption = f"🎵 **صوت MP3**\n@techno_syria_bot"
            await send_file(event.chat_id, output, False, caption)
            os.remove(output)
        elif action == 'gif':
            output = f"{os.path.splitext(file_path)[0]}.gif"
            success, _ = await convert_to_gif(file_path, output)
            if not success:
                raise RuntimeError("فشل تحويل GIF!")
            caption = f"🎬 **GIF متحرك**\n@techno_syria_bot"
            await send_file(event.chat_id, output, False, caption)
            os.remove(output)
        elif action == 'drive':
            link = await upload_to_drive(file_path)
            await event.reply(f"📂 **رابط Drive:** {link}\n@techno_syria_bot")
        elif action == 'telegraph':
            link = await upload_to_telegraph(file_path)
            await event.reply(f"🔗 **رابط Telegraph:** {link}\n@techno_syria_bot")
        stats['downloads'] += 1
        await status_msg.delete()
    except Exception as e:
        stats['errors'] += 1
        await status_msg.edit(f"❌ **فشل المعالجة:** {str(e)}\n@techno_syria_bot")
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

# البحث في يوتيوب
@client.on(events.NewMessage(pattern='/yt (.+)'))
async def youtube_search(event):
    query = event.pattern_match.group(1)
    ydl_opts = {'quiet': True, 'noplaylist': True, 'cookiefile': COOKIES_PATH}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            results = ydl.extract_info(f"ytsearch5:{query}", download=False)['entries']
            if not results:
                raise ValueError("لا توجد نتائج!")
        buttons = [[Button.inline(f"🎥 {res['title'][:30]}", f"yt_select_{res['id']}")] for res in results]
        await event.reply(f"🔎 **نتائج البحث:** {query}\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')
    except Exception as e:
        stats['errors'] += 1
        await event.reply(f"❌ **فشل البحث:** {str(e)}\n@techno_syria_bot")

@client.on(events.CallbackQuery(pattern=r'yt_select_.+'))
async def select_video(event):
    video_id = event.data.decode().split('_')[2]
    buttons = [
        [Button.inline("720p", f"dl_yt_{video_id}_720p"), Button.inline("🎵 MP3", f"dl_yt_{video_id}_mp3")],
        [Button.inline("🎬 GIF", f"dl_yt_{video_id}_gif"), Button.inline("📂 Drive", f"dl_yt_{video_id}_drive")]
    ]
    await event.reply("📏 **اختر خياراً:**\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')

@client.on(events.CallbackQuery(pattern=r'dl_yt_.+'))
async def download_selected(event):
    data = event.data.decode().split('_')
    video_id, option = data[2], data[3]
    url = f"https://youtube.com/watch?v={video_id}"
    format_map = {'720p': 'bestvideo[height<=720]+bestaudio/best[height<=720]', 'mp3': 'bestaudio/best', 'gif': 'bestvideo[height<=720]'}
    await download_media(url, event, 'YouTube', format_map.get(option, '720p'), 
                         audio_only=(option == 'mp3'), to_gif=(option == 'gif'), to_drive=(option == 'drive'))

# إعادة المحاولة
@client.on(events.CallbackQuery(pattern=r'retry_.+'))
async def retry_download(event):
    data = event.data.decode().split('_', 2)
    platform, url = data[1], data[2]
    if platform == 'reels':
        await download_instagram_reels(url, event)
    else:
        await download_media(url, event, platform.capitalize())

# بدء البوت
async def main():
    async for db in get_db():
        await init_db(db)
    if not check_ffmpeg():
        logging.error("FFmpeg missing! Exiting...")
        exit(1)
    if not check_cookies():
        logging.warning("Cookies missing! YouTube may fail.")
    await client.start(bot_token=bot_token)
    print(f"@techno_syria_bot is live! 🚀")
    asyncio.create_task(periodic_cleanup())
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())