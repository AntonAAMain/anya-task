# ============================================================
# Тудулист с уведомлениями в Telegram — весь бэкенд в одном файле
# Запуск: python3 app.py
# ============================================================

import dns.resolver
dns.resolver.default_resolver = dns.resolver.Resolver(configure=False)
dns.resolver.default_resolver.nameservers = ['8.8.8.8', '1.1.1.1']

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import certifi
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, CommandObject
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# ============================================================
# 1. Конфиг
# ============================================================

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_username: str = ""
    mongodb_password: str = ""
    db_name: str = "todo"
    bot_token: str = ""
    host: str = "0.0.0.0"
    port: int = 8000


settings = Settings()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("todo")


# ============================================================
# 2. Mongo (motor)
# ============================================================

client = AsyncIOMotorClient(
    settings.mongodb_uri,
    tlsCAFile=certifi.where(),
)
db = client[settings.db_name]
users_col = db["users"]
tasks_col = db["tasks"]


async def init_indexes():
    await users_col.create_index("username", unique=True)
    await tasks_col.create_index([("is_done", 1), ("next_notify_at", 1)])
    await tasks_col.create_index("user_id")
    log.info("MongoDB indexes ready (db=%s)", settings.db_name)


# ============================================================
# 3. Telegram bot (aiogram) — ОБЪЯВЛЯЕТСЯ ДО lifespan!
# ============================================================

bot: Optional[Bot] = None
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject):
    log.info(">>> /start triggered, args=%r, chat_id=%s", command.args, message.chat.id)
    arg = (command.args or "").strip()
    if not arg:
        await message.answer(
            "Привет! Чтобы привязать аккаунт, отправь:\n"
            "/start твой_username\n\n"
            "username — тот, что ты указал при регистрации в API."
        )
        return

    user = await users_col.find_one({"username": arg})
    if not user:
        await message.answer(
            f"Пользователь <code>{arg}</code> не найден. Сначала зарегистрируйся в API."
        )
        return

    await users_col.update_one(
        {"_id": user["_id"]},
        {"$set": {"telegram_id": message.chat.id}},
    )
    await message.answer(
        f"✅ Аккаунт <b>{arg}</b> привязан к этому чату.\n"
        f"Уведомления по задачам будут приходить сюда."
    )
    log.info("Bound telegram_id=%s to username=%s", message.chat.id, arg)


# ============================================================
# 4. Pydantic-схемы
# ============================================================

class UserRegister(BaseModel):
    username: str = Field(min_length=2, max_length=64)
    password: str = Field(min_length=1, max_length=128)


class UserLogin(BaseModel):
    username: str
    password: str


class TaskCreate(BaseModel):
    user_id: str
    title: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    deadline: Optional[datetime] = None
    notify_before_hours: Optional[int] = None
    notify_interval_minutes: Optional[int] = None


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    deadline: Optional[datetime] = None
    notify_before_hours: Optional[int] = None
    notify_interval_minutes: Optional[int] = None
    is_done: Optional[bool] = None


# ============================================================
# 5. Хелперы
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ensure_aware(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_next_notify_at(
    *,
    deadline: Optional[datetime],
    notify_before_hours: Optional[int],
    notify_interval_minutes: Optional[int],
    created_at: datetime,
    base: Optional[datetime] = None,
) -> Optional[datetime]:
    if deadline and notify_before_hours is not None:
        return deadline - timedelta(hours=notify_before_hours)
    if notify_interval_minutes is not None:
        return (base or created_at) + timedelta(minutes=notify_interval_minutes)
    return None


def task_to_dict(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "user_id": str(doc["user_id"]),
        "title": doc["title"],
        "description": doc.get("description"),
        "deadline": doc.get("deadline"),
        "notify_before_hours": doc.get("notify_before_hours"),
        "notify_interval_minutes": doc.get("notify_interval_minutes"),
        "is_done": doc.get("is_done", False),
        "next_notify_at": doc.get("next_notify_at"),
        "last_notified_at": doc.get("last_notified_at"),
        "created_at": doc.get("created_at"),
    }


def user_to_dict(doc: dict) -> dict:
    return {
        "id": str(doc["_id"]),
        "username": doc["username"],
        "telegram_id": doc.get("telegram_id"),
        "created_at": doc.get("created_at"),
    }


def format_task_message(task: dict) -> str:
    lines = [f"⏰ Напоминание: <b>{task['title']}</b>"]
    if task.get("description"):
        lines.append(f"📝 {task['description']}")
    if task.get("deadline"):
        lines.append(f"🗓 Дедлайн: {task['deadline'].strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"🆔 id: <code>{task['id']}</code>")
    lines.append("\nОтметь как done через API, чтобы прекратить уведомления.")
    return "\n".join(lines)


# ============================================================
# 6. Scheduler
# ============================================================

scheduler: Optional[AsyncIOScheduler] = None


async def check_notifications():
    if bot is None:
        return
    now = now_utc()
    cursor = tasks_col.find({
        "is_done": False,
        "next_notify_at": {"$ne": None, "$lte": now},
    })
    async for task in cursor:
        try:
            user = await users_col.find_one({"_id": task["user_id"]})
            if user and user.get("telegram_id"):
                text = format_task_message(task_to_dict(task))
                try:
                    await bot.send_message(user["telegram_id"], text)
                    log.info("Notified user=%s task=%s", user["username"], task["_id"])
                except Exception as e:
                    log.warning("Failed to send to %s: %s", user["telegram_id"], e)

            update = {"last_notified_at": now}
            interval = task.get("notify_interval_minutes")
            if interval:
                update["next_notify_at"] = now + timedelta(minutes=int(interval))
            else:
                update["next_notify_at"] = None
            await tasks_col.update_one({"_id": task["_id"]}, {"$set": update})
        except Exception as e:
            log.exception("Error processing task %s: %s", task.get("_id"), e)


# ============================================================
# 7. FastAPI + lifespan
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot, scheduler

    await init_indexes()

    if settings.bot_token:
        bot = Bot(token=settings.bot_token)
        try:
            me = await bot.get_me()
            log.info("Bot @%s connected", me.username)
        except Exception as e:
            log.error("Bot token invalid: %s", e)
            await bot.session.close()
            bot = None

        if bot is not None:
            polling_task = asyncio.create_task(dp.start_polling(bot))
        else:
            polling_task = None
    else:
        log.warning("BOT_TOKEN not set — бот отключён, уведомления не шлются")
        polling_task = None

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(check_notifications, "interval", minutes=1, id="notify")
    scheduler.start()
    log.info("Scheduler started")

    try:
        yield
    finally:
        if scheduler:
            scheduler.shutdown(wait=False)
        if polling_task:
            polling_task.cancel()
            try:
                await polling_task
            except (asyncio.CancelledError, Exception):
                pass
        if bot:
            await bot.session.close()
        client.close()
        log.info("Shutdown complete")


app = FastAPI(title="Todo Bot API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# 8. API: users
# ============================================================

@app.post("/users/register")
async def register(payload: UserRegister):
    existing = await users_col.find_one({"username": payload.username})
    if existing:
        raise HTTPException(400, "Username already taken")
    doc = {
        "username": payload.username,
        "password": payload.password,
        "telegram_id": None,
        "created_at": now_utc(),
    }
    res = await users_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    return user_to_dict(doc)


@app.post("/users/login")
async def login(payload: UserLogin):
    user = await users_col.find_one(
        {"username": payload.username, "password": payload.password}
    )
    if not user:
        raise HTTPException(401, "Invalid credentials")
    return user_to_dict(user)


# ============================================================
# 9. API: tasks
# ============================================================

@app.get("/users/{user_id}/tasks")
async def list_tasks(user_id: str):
    from bson import ObjectId
    if not ObjectId.is_valid(user_id):
        raise HTTPException(400, "Bad user_id")
    cursor = tasks_col.find({"user_id": ObjectId(user_id)}).sort("created_at", -1)
    return [task_to_dict(t) async for t in cursor]


@app.post("/tasks")
async def create_task(payload: TaskCreate):
    from bson import ObjectId
    if not ObjectId.is_valid(payload.user_id):
        raise HTTPException(400, "Bad user_id")
    user = await users_col.find_one({"_id": ObjectId(payload.user_id)})
    if not user:
        raise HTTPException(404, "User not found")

    deadline = ensure_aware(payload.deadline)
    created_at = now_utc()
    next_notify = compute_next_notify_at(
        deadline=deadline,
        notify_before_hours=payload.notify_before_hours,
        notify_interval_minutes=payload.notify_interval_minutes,
        created_at=created_at,
    )

    doc = {
        "user_id": ObjectId(payload.user_id),
        "title": payload.title,
        "description": payload.description,
        "deadline": deadline,
        "notify_before_hours": payload.notify_before_hours,
        "notify_interval_minutes": payload.notify_interval_minutes,
        "is_done": False,
        "next_notify_at": next_notify,
        "last_notified_at": None,
        "created_at": created_at,
    }
    res = await tasks_col.insert_one(doc)
    doc["_id"] = res.inserted_id
    return task_to_dict(doc)


@app.patch("/tasks/{task_id}")
async def update_task(task_id: str, payload: TaskUpdate):
    from bson import ObjectId
    if not ObjectId.is_valid(task_id):
        raise HTTPException(400, "Bad task_id")
    task = await tasks_col.find_one({"_id": ObjectId(task_id)})
    if not task:
        raise HTTPException(404, "Task not found")

    update = {}
    if payload.title is not None:
        update["title"] = payload.title
    if payload.description is not None:
        update["description"] = payload.description
    if payload.deadline is not None:
        update["deadline"] = ensure_aware(payload.deadline)
    if payload.notify_before_hours is not None:
        update["notify_before_hours"] = payload.notify_before_hours
    if payload.notify_interval_minutes is not None:
        update["notify_interval_minutes"] = payload.notify_interval_minutes
    if payload.is_done is not None:
        update["is_done"] = payload.is_done
        if payload.is_done:
            update["next_notify_at"] = None

    if any(k in update for k in ("deadline", "notify_before_hours", "notify_interval_minutes")) and not update.get("is_done"):
        merged_deadline = update.get("deadline", task.get("deadline"))
        merged_before = update.get("notify_before_hours", task.get("notify_before_hours"))
        merged_interval = update.get("notify_interval_minutes", task.get("notify_interval_minutes"))
        update["next_notify_at"] = compute_next_notify_at(
            deadline=merged_deadline,
            notify_before_hours=merged_before,
            notify_interval_minutes=merged_interval,
            created_at=task.get("created_at", now_utc()),
            base=now_utc(),
        )

    if update:
        await tasks_col.update_one({"_id": ObjectId(task_id)}, {"$set": update})

    task = await tasks_col.find_one({"_id": ObjectId(task_id)})
    return task_to_dict(task)


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    from bson import ObjectId
    if not ObjectId.is_valid(task_id):
        raise HTTPException(400, "Bad task_id")
    res = await tasks_col.delete_one({"_id": ObjectId(task_id)})
    if res.deleted_count == 0:
        raise HTTPException(404, "Task not found")
    return {"ok": True}


# ============================================================
# 10. Точка входа
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.host, port=settings.port, log_level="info")