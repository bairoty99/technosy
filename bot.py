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

# التحقق من الإعدادات
if not api_id or not api_hash or not bot_token:
    logging.error("Missing API credentials! Check your .env file.")
    exit(1)

# إعداد العميل
client = TelegramClient('TechnoSyriaBot', api_id, api_hash)

# أنماط الروابط
YT_PATTERN = r'https?://(?:www\.)?(youtube|youtu\.be)[^\s]+'
INSTA_PATTERN = r'https?://(?:www\.)?instagram\.com/(p|reel|stories|highlights|tv)[^\s]+'
TIKTOK_PATTERN = r'https?://(?:www\.)?(tiktok\.com|vm\.tiktok\.com)[^\s]+'
FB_PATTERN = r'https?://(?:www\.)?(facebook\.com|fb\.watch)[^\s]+'
TWITTER_PATTERN = r'https?://(?:www\.)?(twitter\.com|x\.com)[^\s]+'
TELEGRAM_STORY_PATTERN = r'https?://t\.me/[^/]+/s/(\d+)'

# طابور المهام والإحصائيات
download_queue = Queue()
stats = {'downloads': 0, 'errors': 0}
banned_users = set()
muted_users = set()
semaphore = Semaphore(3)
active_downloads = {}
telegraph = Telegraph()
telegraph.create_account(short_name='TechnoSyriaBot')

# Google Drive
drive_service = None
if GOOGLE_CREDS and os.path.exists(GOOGLE_CREDS):
    try:
        creds = Credentials.from_authorized_user_file(GOOGLE_CREDS)
        drive_service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.error(f"Google Drive credentials invalid: {str(e)}")

# جلسة aiosqlite
async def get_db():
    try:
        db = await aiosqlite.connect('cache.db')
        yield db
    except aiosqlite.Error as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise
    finally:
        await db.close()

async def init_db(db):
    await db.execute('''CREATE TABLE IF NOT EXISTS cache (url TEXT PRIMARY KEY, file_path TEXT, timestamp REAL)''')
    await db.commit()

# تنظيف الملفات دورياً مع تحقق الحجم
async def periodic_cleanup():
    while True:
        await asyncio.sleep(3600)  # كل ساعة
        total_size = sum(os.path.getsize(f) for f in Path("downloads").glob("**/*") if f.is_file()) / (1024 * 1024)
        if total_size > 500:  # 500MB حد أقصى
            async for db in get_db():
                async with db.execute("SELECT file_path, timestamp FROM cache WHERE timestamp < ?", 
                                     (time.time() - 24*3600,)) as cursor:
                    rows = await cursor.fetchall()
                for row in rows:
                    file_path, _ = row
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    async with db.execute("DELETE FROM cache WHERE file_path = ?", (file_path,)):
                        await db.commit()
            logging.info(f"Cleaned up {total_size:.2f}MB of unused files.")

# التحقق من وجود ffmpeg
def check_ffmpeg():
    return shutil.which("ffmpeg") is not None

# أمر /start
@client.on(events.NewMessage(pattern='/start'))
async def start_command(event):
    welcome_msg = (
        "🌌 **@techno_syria_bot الأسطورة الخارقة!** 🌌\n"
        "تحميل فيديوهات، صور، قوائم تشغيل، وستوريات تلغرام بسهولة ⚡\n\n"
        "🔹 `/yt [اسم]` | `/help` | `/stats` | `/cancel`\n"
        "🔹 أرسل رابط واختر خياراتك!"
    )
    buttons = [
        [Button.inline("📹 يوتيوب", "yt_help"), Button.inline("📸 إنستغرام", "insta_help")],
        [Button.inline("🎬 تيك توك", "tiktok_help"), Button.inline("📱 فيسبوك", "fb_help")],
        [Button.inline("🐦 تويتر", "twitter_help"), Button.inline("📖 ستوري تلغرام", "telegram_help")],
        [Button.inline("⚙️ أدوات", "tools_help"), Button.inline("ℹ️ الحالة", "status")]
    ]
    await event.reply(welcome_msg, buttons=buttons, parse_mode='markdown')

# أوامر إضافية
@client.on(events.NewMessage(pattern='/help'))
async def help_command(event):
    msg = (
        "📚 **دليل @techno_syria_bot**\n"
        "🔹 `/start` - بدء البوت\n"
        "🔹 `/yt [اسم]` - بحث مع فلاتر\n"
        "🔹 `/stats` - إحصائيات\n"
        "🔹 `/cancel` - إلغاء التحميل\n"
        "🔹 يدعم Google Drive، Telegraph، وستوريات تلغرام!"
    )
    await event.reply(msg, parse_mode='markdown')

@client.on(events.NewMessage(pattern='/stats'))
async def stats_command(event):
    total_size = sum(os.path.getsize(f) for f in Path("downloads").glob("**/*") if f.is_file()) / (1024 * 1024)
    msg = (
        f"📊 **إحصائيات @techno_syria_bot**\n"
        f"🔹 التحميلات: {stats['downloads']}\n"
        f"🔹 الأخطاء: {stats['errors']}\n"
        f"🔹 المحظورون: {len(banned_users)}\n"
        f"🔹 حجم المؤقت: {total_size:.2f}MB"
    )
    await event.reply(msg, parse_mode='markdown')

@client.on(events.NewMessage(pattern='/cancel'))
async def cancel_command(event):
    if event.sender_id in active_downloads:
        active_downloads[event.sender_id].cancel()
        del active_downloads[event.sender_id]
        await event.reply("🛑 **تم إلغاء التحميل!**\n@techno_syria_bot", parse_mode='markdown')
    else:
        await event.reply("❌ **لا يوجد تحميل نشط!**\n@techno_syria_bot", parse_mode='markdown')

# مساعدة وحالة
@client.on(events.CallbackQuery(pattern=r'(yt|insta|tiktok|fb|twitter|telegram|tools|status)_help'))
async def help_handler(event):
    platform = event.data.decode().split('_')[0]
    messages = {
        'yt': "📹 **يوتيوب**: قوائم تشغيل و4K!",
        'insta': "📸 **إنستغرام**: فيديوهات وصور!",
        'tiktok': "🎬 **تيك توك**: أرسل رابط!",
        'fb': "📱 **فيسبوك**: أرسل رابط!",
        'twitter': "🐦 **تويتر**: فيديوهات وصور!",
        'telegram': "📖 **ستوري تلغرام**: أرسل رابط الستوري!",
        'tools': "⚙️ **أدوات**: Drive، Telegraph، GIF!",
        'status': f"ℹ️ **الحالة**\nFFmpeg: {'✅' if check_ffmpeg() else '❌'}\nDrive: {'✅' if drive_service else '❌'}\n@techno_syria_bot"
    }
    await event.reply(messages[platform], parse_mode='markdown')

# تقسيم الملفات مع تحقق
async def split_file(file_path, chunk_size=50*1024*1024):
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    if file_size > 2000:  # 2GB حد تلغرام
        raise ValueError("File too large for Telegram (max 2GB)!")
    async with aiofiles.open(file_path, 'rb') as f:
        content = await f.read()
    parts = []
    for i in range(0, len(content), chunk_size):
        part_path = f"{file_path}.part{i//chunk_size}"
        async with aiofiles.open(part_path, 'wb') as part_file:
            await part_file.write(content[i:i + chunk_size])
        parts.append(part_path)
    return parts

# تشغيل ffmpeg محسّن
async def run_ffmpeg(cmd, timeout=300):
    if not check_ffmpeg():
        raise RuntimeError("FFmpeg is not installed!")
    args = shlex.split(cmd)
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        if process.returncode != 0:
            error_msg = stderr.decode().strip()
            logging.error(f"FFmpeg Error: {error_msg}")
            return False, error_msg
        return True, ""
    except asyncio.TimeoutError:
        process.kill()
        logging.error(f"FFmpeg timed out after {timeout}s!")
        return False, f"FFmpeg timed out after {timeout}s. Try a smaller file."
    except Exception as e:
        logging.error(f"FFmpeg unexpected error: {str(e)}")
        return False, str(e)

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

# إرسال الملف مع إعادة محاولة
async def send_file_properly(chat, file, as_doc=False, caption="", retries=3):
    for attempt in range(retries):
        try:
            await client.send_file(chat, file, force_document=as_doc, caption=caption, parse_mode='markdown')
            return True
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(5)
                continue
            logging.error(f"Failed to send file after {retries} retries: {str(e)}")
            return False

# رفع إلى Google Drive
async def upload_to_drive(file_path):
    if not drive_service:
        return None
    file_metadata = {'name': os.path.basename(file_path)}
    media = MediaFileUpload(file_path)
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id,webViewLink').execute()
    return file.get('webViewLink')

# إعادة المحاولة
async def retry_on_failure(func, *args, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return await func(*args)
        except (yt_dlp.DownloadError, instaloader.exceptions.ConnectionException, asyncio.TimeoutError) as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                continue
            raise e

# تحقق من الرابط
def validate_url(url):
    return validators.url(url) and bool(urlparse(url).scheme)

# تحميل عام مع طابور
async def download_media(url, event, platform, quality='best', audio_only=False, as_document=False, to_gif=False, share_link=False, to_drive=False, is_playlist=False):
    if event.sender_id in banned_users:
        await event.reply("❌ **تم حظرك!**\n@techno_syria_bot")
        return
    if event.sender_id in muted_users:
        return
    if not validate_url(url):
        await event.reply("❌ **رابط غير صالح!**\n@techno_syria_bot")
        return

    async with semaphore:
        task = asyncio.create_task(process_download(url, event, platform, quality, audio_only, as_document, to_gif, share_link, to_drive, is_playlist))
        active_downloads[event.sender_id] = task
        try:
            await task
        except asyncio.CancelledError:
            await event.reply("🛑 **تم إلغاء التحميل!**\n@techno_syria_bot")
        except Exception as e:
            await event.reply(f"❌ **خطأ غير متوقع:** {str(e)}\n@techno_syria_bot")
        finally:
            if event.sender_id in active_downloads:
                del active_downloads[event.sender_id]

async def process_download(url, event, platform, quality, audio_only, as_document, to_gif, share_link, to_drive, is_playlist):
    status_msg = await event.reply(f"⚡ **بدء تحميل من {platform}{' (قائمة تشغيل)' if is_playlist else ''}...** ⏳", parse_mode='markdown')
    async for db in get_db():
        async with db.execute("SELECT file_path FROM cache WHERE url=?", (url,)) as cursor:
            cached = await cursor.fetchone()
        if cached and os.path.exists(cached[0]) and os.path.getsize(cached[0]) > 0 and not is_playlist:
            file_path = cached[0]
            files = [file_path]
        else:
            ydl_opts = {
                'format': 'bestvideo[height<=720]+bestaudio/best[height<=720]' if quality == 'best' else quality,
                'outtmpl': 'downloads/%(title)s.%(ext)s',
                'quiet': True,
                'merge_output_format': 'mp4',
                'max_filesize': 2 * 1024 * 1024 * 1024,
                'noplaylist': not is_playlist,
            }
            if audio_only:
                ydl_opts['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
                del ydl_opts['merge_output_format']
            try:
                await status_msg.edit(f"⚡ **جاري التحميل (1/3)...** ⏳")
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = await retry_on_failure(lambda: ydl.extract_info(url, download=True))
                    if is_playlist:
                        files = [f"downloads/{slugify(entry['title'])}.{'mp3' if audio_only else 'mp4'}" for entry in info['entries'] if entry]
                    else:
                        if not info.get('formats') or not info.get('title'):
                            raise ValueError("No valid media found!")
                        file_path = f"downloads/{slugify(info['title'])}.{'mp3' if audio_only else 'mp4'}"
                        files = [file_path]
                await status_msg.edit(f"⚡ **جاري المعالجة (2/3)...** ⏳")
                processed_files = []
                for file in files:
                    if not audio_only and not to_gif:
                        compressed_path = f"{os.path.splitext(file)[0]}_compressed.mp4"
                        success, error = await compress_video(file, compressed_path)
                        if success:
                            os.remove(file)
                            processed_files.append(compressed_path)
                        else:
                            await status_msg.edit(f"❌ **خطأ في الضغط:** {error}\n@techno_syria_bot")
                            return
                    elif to_gif:
                        gif_path = f"{os.path.splitext(file)[0]}.gif"
                        success, error = await convert_to_gif(file, gif_path)
                        if success:
                            os.remove(file)
                            processed_files.append(gif_path)
                        else:
                            await status_msg.edit(f"❌ **خطأ في تحويل GIF:** {error}\n@techno_syria_bot")
                            return
                    else:
                        processed_files.append(file)
                if not is_playlist:
                    async with db.execute("INSERT OR REPLACE INTO cache (url, file_path, timestamp) VALUES (?, ?, ?)", 
                                         (url, processed_files[0], time.time())):
                        await db.commit()
            except Exception as e:
                stats['errors'] += 1
                for file in files:
                    if os.path.exists(file) and os.path.getsize(file) == 0:
                        os.remove(file)
                await status_msg.edit(f"❌ **خطأ:** {str(e)}\n@techno_syria_bot", 
                                     buttons=[Button.inline("🔄 إعادة المحاولة", f"retry_{platform}_{url}")])
                await client.send_message(DEVELOPER_ID, f"⚠️ **خطأ {platform}:** {str(e)} من {event.sender_id}")
                return
        try:
            await status_msg.edit(f"⚡ **جاري الإرسال (3/3)...** ⏳")
            for file in processed_files:
                size = os.path.getsize(file) / (1024 * 1024)
                caption = f"{'🎵' if audio_only else '🎬' if to_gif else '🎥'} **{os.path.basename(file).split('.')[0]}**\n📥 @techno_syria_bot"
                if share_link:
                    with open(file, 'rb') as f:
                        response = telegraph.upload_file(f)
                        temp_link = f"https://telegra.ph{response[0]['src']}"
                    await event.reply(f"🔗 **رابط Telegraph:** {temp_link}\n@techno_syria_bot")
                elif to_drive and drive_service:
                    drive_link = await upload_to_drive(file)
                    await event.reply(f"📂 **رابط Google Drive:** {drive_link}\n@techno_syria_bot")
                elif size > 50:
                    try:
                        parts = await split_file(file)
                        await event.reply(f"📦 **سيتم إرسال الملف بـ{len(parts)} أجزاء...**", parse_mode='markdown')
                        for i, part in enumerate(parts, 1):
                            if not await send_file_properly(event.chat_id, part, as_document, f"📦 **جزء {i}/{len(parts)}**\n{caption}"):
                                raise Exception("Failed to send file part!")
                            os.remove(part)
                    except ValueError as e:
                        await event.reply(f"❌ **خطأ:** {str(e)}\n@techno_syria_bot")
                        return
                else:
                    if not await send_file_properly(event.chat_id, file, as_document, caption):
                        raise Exception("Failed to send file!")
                stats['downloads'] += 1
                if not cached or is_playlist:
                    os.remove(file)
            await status_msg.delete()
        except Exception as e:
            stats['errors'] += 1
            await status_msg.edit(f"❌ **خطأ في الإرسال:** {str(e)}\n@techno_syria_bot")
            await client.send_message(DEVELOPER_ID, f"⚠️ **خطأ إرسال:** {str(e)} من {event.sender_id}")

# تحميل إنستغرام
async def download_instagram(url, event, as_document=False, share_link=False, to_drive=False):
    if not validate_url(url):
        await event.reply("❌ **رابط غير صالح!**\n@techno_syria_bot")
        return
    async with semaphore:
        task = asyncio.create_task(process_instagram(url, event, as_document, share_link, to_drive))
        active_downloads[event.sender_id] = task
        try:
            await task
        finally:
            if event.sender_id in active_downloads:
                del active_downloads[event.sender_id]

async def process_instagram(url, event, as_document, share_link, to_drive):
    status_msg = await event.reply("⚡ **بدء تحميل من إنستغرام...** ⏳", parse_mode='markdown')
    match = re.search(INSTA_PATTERN, url)
    if not match:
        stats['errors'] += 1
        await status_msg.edit("❌ **خطأ:** رابط إنستغرام غير صالح!\n@techno_syria_bot")
        return
    shortcode = match.group(2)
    file_path = None
    with ProcessPoolExecutor(max_workers=1) as executor:  # تقليل العمال
        try:
            await status_msg.edit("⚡ **جاري التحميل (1/2)...** ⏳")
            L = instaloader.Instaloader(dirname_pattern="downloads/{shortcode}", download_comments=False, save_metadata=False)
            post = await asyncio.get_event_loop().run_in_executor(executor, lambda: instaloader.Post.from_shortcode(L.context, shortcode))
            if post.is_video and not post.video_url:
                raise instaloader.exceptions.PrivateProfileNotFollowedException("Private content!")
            await asyncio.get_event_loop().run_in_executor(executor, lambda: L.download_post(post, "downloads"))
            file_path = f"downloads/{shortcode}/{shortcode}.mp4" if post.is_video else f"downloads/{shortcode}/{shortcode}.jpg"
            await status_msg.edit("⚡ **جاري الإرسال (2/2)...** ⏳")
            caption = f"{'🎥' if post.is_video else '🖼️'} **{post.caption[:50] + '...' if post.caption else 'بدون عنوان'}**\n@techno_syria_bot"
            if share_link:
                with open(file_path, 'rb') as f:
                    response = telegraph.upload_file(f)
                    temp_link = f"https://telegra.ph{response[0]['src']}"
                await status_msg.edit(f"🔗 **رابط Telegraph:** {temp_link}\n@techno_syria_bot")
            elif to_drive and drive_service:
                drive_link = await upload_to_drive(file_path)
                await status_msg.edit(f"📂 **رابط Google Drive:** {drive_link}\n@techno_syria_bot")
            else:
                await send_file_properly(event.chat_id, file_path, as_document, caption)
            stats['downloads'] += 1
            await status_msg.delete()
        except instaloader.exceptions.PrivateProfileNotFollowedException:
            stats['errors'] += 1
            await status_msg.edit("❌ **خطأ:** المحتوى خاص!\n@techno_syria_bot")
        except Exception as e:
            stats['errors'] += 1
            if file_path and os.path.exists(file_path) and os.path.getsize(file_path) == 0:
                os.remove(file_path)
            await status_msg.edit(f"❌ **خطأ:** {str(e)}\n@techno_syria_bot", 
                                 buttons=[Button.inline("🔄 إعادة المحاولة", f"retry_insta_{url}")])
        finally:
            if file_path and os.path.exists(os.path.dirname(file_path)):
                shutil.rmtree(os.path.dirname(file_path))

# تحميل ستوريات تلغرام
async def download_telegram_story(url, event, as_document=False, share_link=False, to_drive=False):
    if not validate_url(url):
        await event.reply("❌ **رابط غير صالح!**\n@techno_syria_bot")
        return
    async with semaphore:
        task = asyncio.create_task(process_telegram_story(url, event, as_document, share_link, to_drive))
        active_downloads[event.sender_id] = task
        try:
            await task
        finally:
            if event.sender_id in active_downloads:
                del active_downloads[event.sender_id]

async def process_telegram_story(url, event, as_document, share_link, to_drive):
    status_msg = await event.reply("⚡ **بدء تحميل ستوري تلغرام...** ⏳", parse_mode='markdown')
    match = re.search(TELEGRAM_STORY_PATTERN, url)
    if not match:
        stats['errors'] += 1
        await status_msg.edit("❌ **خطأ:** رابط ستوري غير صالح!\n@techno_syria_bot")
        return
    story_id = int(match.group(1))
    try:
        await status_msg.edit("⚡ **جاري التحميل (1/2)...** ⏳")
        entity = await client.get_entity(url.split('/s/')[0].replace('https://t.me/', ''))
        async for story in client.iter_stories(entity.id):
            if story.id == story_id:
                file_path = f"downloads/telegram_story_{story_id}.{'mp4' if story.video else 'jpg'}"
                await client.download_media(story, file_path)
                break
        else:
            raise ValueError("Story not found!")
        await status_msg.edit("⚡ **جاري الإرسال (2/2)...** ⏳")
        caption = f"📖 **ستوري تلغرام**\n@techno_syria_bot"
        if share_link:
            with open(file_path, 'rb') as f:
                response = telegraph.upload_file(f)
                temp_link = f"https://telegra.ph{response[0]['src']}"
            await status_msg.edit(f"🔗 **رابط Telegraph:** {temp_link}\n@techno_syria_bot")
        elif to_drive and drive_service:
            drive_link = await upload_to_drive(file_path)
            await status_msg.edit(f"📂 **رابط Google Drive:** {drive_link}\n@techno_syria_bot")
        else:
            await send_file_properly(event.chat_id, file_path, as_document, caption)
        stats['downloads'] += 1
        await status_msg.delete()
        os.remove(file_path)
    except Exception as e:
        stats['errors'] += 1
        await status_msg.edit(f"❌ **خطأ:** {str(e)}\n@techno_syria_bot", 
                             buttons=[Button.inline("🔄 إعادة المحاولة", f"retry_telegram_{url}")])

# معالجة الروابط
@client.on(events.NewMessage)
async def handle_links(event):
    text = event.raw_text
    tasks = []
    if re.search(YT_PATTERN, text):
        is_playlist = 'playlist' in text.lower() or 'list=' in text
        tasks.append(download_media(text, event, 'YouTube', is_playlist=is_playlist))
    if re.search(INSTA_PATTERN, text):
        tasks.append(download_instagram(text, event))
    if re.search(TIKTOK_PATTERN, text):
        tasks.append(download_media(text, event, 'TikTok'))
    if re.search(FB_PATTERN, text):
        tasks.append(download_media(text, event, 'Facebook'))
    if re.search(TWITTER_PATTERN, text):
        tasks.append(download_media(text, event, 'Twitter'))
    if re.search(TELEGRAM_STORY_PATTERN, text):
        tasks.append(download_telegram_story(text, event))
    if tasks:
        await asyncio.gather(*tasks)

# البحث في يوتيوب مع فلاتر
@client.on(events.NewMessage(pattern='/yt (.+)'))
async def youtube_search(event):
    if event.sender_id in banned_users or event.sender_id in muted_users:
        return
    query = event.pattern_match.group(1)
    ydl_opts = {'quiet': True, 'noplaylist': True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            results = ydl.extract_info(f"ytsearch10:{query}", download=False)['entries']
            if not results:
                raise ValueError("No results found!")
        buttons = [[Button.inline(f"🎥 {i+1}. {res['title'][:30]} | {res['duration']//60}:{res['duration']%60:02d}", f"yt_select_{res['id']}")] for i, res in enumerate(results[:5])]
        buttons.append([
            Button.inline("⏱️ الأقصر", f"yt_filter_{query}_duration_short"),
            Button.inline("👀 الأكثر مشاهدة", f"yt_filter_{query}_views")
        ])
        if len(results) > 5:
            buttons.append([Button.inline("➡️ التالي", f"yt_next_{query}_5")])
        await event.reply(f"🔎 **نتائج البحث:** {query}\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')
    except Exception as e:
        stats['errors'] += 1
        await event.reply(f"❌ **خطأ في البحث:** {str(e)}\n@techno_syria_bot")

@client.on(events.CallbackQuery(pattern=r'yt_filter_.+'))
async def filter_results(event):
    _, query, filter_type = event.data.decode().split('_', 2)
    ydl_opts = {'quiet': True, 'noplaylist': True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            results = ydl.extract_info(f"ytsearch10:{query}", download=False)['entries']
            if filter_type == 'duration_short':
                results.sort(key=lambda x: x['duration'])
            elif filter_type == 'views':
                results.sort(key=lambda x: x.get('view_count', 0), reverse=True)
        buttons = [[Button.inline(f"🎥 {i+1}. {res['title'][:30]} | {res['duration']//60}:{res['duration']%60:02d}", f"yt_select_{res['id']}")] for i, res in enumerate(results[:5])]
        buttons.append([
            Button.inline("⏱️ الأقصر", f"yt_filter_{query}_duration_short"),
            Button.inline("👀 الأكثر مشاهدة", f"yt_filter_{query}_views")
        ])
        await event.edit(f"🔎 **نتائج مفلترة:** {query}\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')
    except Exception as e:
        stats['errors'] += 1
        await event.reply(f"❌ **خطأ في الفلترة:** {str(e)}\n@techno_syria_bot")

@client.on(events.CallbackQuery(pattern=r'yt_next_.+'))
async def next_results(event):
    _, query, offset = event.data.decode().split('_')
    offset = int(offset)
    ydl_opts = {'quiet': True, 'noplaylist': True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            results = ydl.extract_info(f"ytsearch10:{query}", download=False)['entries']
        buttons = [[Button.inline(f"🎥 {i+1+offset}. {res['title'][:30]} | {res['duration']//60}:{res['duration']%60:02d}", f"yt_select_{res['id']}")] for i, res in enumerate(results[offset:offset+5])]
        buttons.append([
            Button.inline("⏱️ الأقصر", f"yt_filter_{query}_duration_short"),
            Button.inline("👀 الأكثر مشاهدة", f"yt_filter_{query}_views")
        ])
        if len(results) > offset + 5:
            buttons.append([Button.inline("➡️ التالي", f"yt_next_{query}_{offset+5}")])
        await event.edit(f"🔎 **نتائج البحث:** {query}\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')
    except Exception as e:
        stats['errors'] += 1
        await event.reply(f"❌ **خطأ في البحث:** {str(e)}\n@techno_syria_bot")

# اختيار الفيديو
@client.on(events.CallbackQuery(pattern=r'yt_select_.+'))
async def select_video(event):
    video_id = event.data.decode().split('_')[2]
    buttons = [
        [Button.inline("480p", f"dl_yt_{video_id}_480p"), Button.inline("720p", f"dl_yt_{video_id}_720p")],
        [Button.inline("1080p", f"dl_yt_{video_id}_1080p"), Button.inline("4K", f"dl_yt_{video_id}_4k")],
        [Button.inline("🎵 MP3", f"dl_yt_{video_id}_mp3"), Button.inline("🎞️ GIF", f"dl_yt_{video_id}_gif")],
        [Button.inline("📜 مستند", f"dl_yt_{video_id}_best_doc"), Button.inline("🔗 Telegraph", f"dl_yt_{video_id}_link")],
        [Button.inline("📂 Drive", f"dl_yt_{video_id}_drive"), Button.inline("📋 Playlist", f"dl_yt_{video_id}_playlist")]
    ]
    await event.reply("📏 **اختر الخيار:**\n@techno_syria_bot", buttons=buttons, parse_mode='markdown')

# تحميل الفيديو
@client.on(events.CallbackQuery(pattern=r'dl_yt_.+'))
async def download_selected(event):
    data = event.data.decode().split('_')
    video_id = data[2]
    option = data[3]
    url = f"https://youtube.com/watch?v={video_id}"
    format_map = {
        '480p': 'bestvideo[height<=480]+bestaudio/best[height<=480]',
        '720p': 'bestvideo[height<=720]+bestaudio/best[height<=720]',
        '1080p': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]',
        '4k': 'bestvideo[height<=2160]+bestaudio/best[height<=2160]',
        'best': 'bestvideo[height<=720]+bestaudio/best[height<=720]',
        'mp3': 'bestaudio/best',
        'gif': 'bestvideo[height<=720]+bestaudio/best[height<=720]',
        'playlist': 'bestvideo[height<=720]+bestaudio/best[height<=720]'
    }
    audio_only = option == 'mp3'
    as_document = option == 'best_doc'
    to_gif = option == 'gif'
    share_link = option == 'link'
    to_drive = option == 'drive'
    is_playlist = option == 'playlist'
    await download_media(url, event, 'YouTube', format_map.get(option, 'best'), audio_only, as_document, to_gif, share_link, to_drive, is_playlist)

# إعادة المحاولة
@client.on(events.CallbackQuery(pattern=r'retry_.+'))
async def retry_download(event):
    data = event.data.decode().split('_', 2)
    platform, url = data[1], data[2]
    if platform == 'insta':
        await download_instagram(url, event)
    elif platform == 'telegram':
        await download_telegram_story(url, event)
    else:
        await download_media(url, event, platform.capitalize())

# بدء البوت
async def main():
    async for db in get_db():
        await init_db(db)
    try:
        await client.start(bot_token=bot_token)
        me = await client.get_me()
        logging.info(f"TechnoSyriaBot is running! Username: @{me.username}")
        print(f"@techno_syria_bot is live and unbeatable! 🚀")
        asyncio.create_task(periodic_cleanup())
        await client.run_until_disconnected()
    except Exception as e:
        logging.error(f"Startup error: {str(e)}")
        await client.send_message(DEVELOPER_ID, f"⚠️ **خطأ تشغيل:** {str(e)}")
        print(f"Startup failed: {str(e)}")

if __name__ == '__main__':
    asyncio.run(main())
