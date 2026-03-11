import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import SNAPSHOT_INTERVAL_MINUTES, DEEP_INTEGRITY_ENABLED, DEEP_INTEGRITY_INTERVAL_HOURS
from collector import collect_snapshots, refresh_symbols

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def start_scheduler():
    global _scheduler
    if _scheduler is not None:
        return

    _scheduler = AsyncIOScheduler()

    # Refresh symbols every 6 hours
    _scheduler.add_job(
        _run_refresh_symbols,
        trigger=IntervalTrigger(hours=6),
        id="refresh_symbols",
        name="Refresh exchange symbol lists",
        replace_existing=True,
    )

    # Collect funding rate snapshots every N minutes (matches loris frequency)
    _scheduler.add_job(
        _run_collect_snapshots,
        trigger=IntervalTrigger(minutes=SNAPSHOT_INTERVAL_MINUTES),
        id="collect_snapshots",
        name="Collect funding rate snapshots",
        replace_existing=True,
    )

    # Deep integrity check — per-exchange gap detection + loris fill
    if DEEP_INTEGRITY_ENABLED:
        _scheduler.add_job(
            _run_deep_integrity,
            trigger=IntervalTrigger(hours=DEEP_INTEGRITY_INTERVAL_HOURS),
            id="deep_integrity",
            name="Deep integrity check",
            replace_existing=True,
        )

    _scheduler.start()
    jobs = f"symbols every 6h, snapshots every {SNAPSHOT_INTERVAL_MINUTES}min"
    if DEEP_INTEGRITY_ENABLED:
        jobs += f", deep integrity every {DEEP_INTEGRITY_INTERVAL_HOURS}h"
    logger.info(f"Scheduler started: {jobs}")


async def _run_refresh_symbols():
    try:
        await refresh_symbols()
    except Exception as e:
        logger.error(f"Scheduled symbol refresh failed: {e}")


async def _run_collect_snapshots():
    try:
        await collect_snapshots()
    except Exception as e:
        logger.error(f"Scheduled snapshot collection failed: {e}")


async def _run_deep_integrity():
    try:
        from integrity import deep_integrity_check
        await deep_integrity_check(fix=True)
    except Exception as e:
        logger.error(f"Scheduled deep integrity check failed: {e}")


def stop_scheduler():
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        _scheduler = None
