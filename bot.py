#!/usr/bin/env python3
"""Nox Streamer — multi-session Discord stream bot."""
from __future__ import annotations

import asyncio
import os
import threading
import time

import discord.errors
if not hasattr(discord.errors, "MissingApplicationID"):
    try:
        from discord.app_commands.errors import MissingApplicationID
        discord.errors.MissingApplicationID = MissingApplicationID
    except ImportError:
        class DummyMissingApplicationID(Exception):
            pass
        discord.errors.MissingApplicationID = DummyMissingApplicationID

import discord
from discord import app_commands
from discord.ext import commands

from stream import (
    Console, Session, State,
    GatewayStreamEngine,
    ensure_token,
    validate_viewer_token,
    enable_personalization,
    STREAM_TIERS,
)

EMBED_COLOR = 0x000000


def _load_env(path=".env"):
    if not os.path.exists(path): return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
GUILD_ID = int(os.getenv("GUILD_ID", "0") or 0)
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
PREFIX = os.getenv("BOT_PREFIX", "!")
LOCK_ID = int(os.getenv("LOCK_ID", "0") or 0)

THUMBNAIL_URL = os.getenv("THUMBNAIL_URL", "").strip()

THUMBNAIL_FILE: str | None = None
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
for _ext in ("png", "jpg", "jpeg", "gif", "webp"):
    _candidate = os.path.join(_BASE_DIR, f"thumbnail.{_ext}")
    if os.path.isfile(_candidate):
        THUMBNAIL_FILE = _candidate
        break


def _with_thumb(embed: discord.Embed | None) -> tuple[discord.Embed | None, list]:
    files: list = []
    if embed is None:
        return None, files
    if embed.thumbnail and embed.thumbnail.url:
        return embed, files
    if THUMBNAIL_URL:
        embed.set_thumbnail(url=THUMBNAIL_URL)
    elif THUMBNAIL_FILE:
        filename = os.path.basename(THUMBNAIL_FILE)
        embed.set_thumbnail(url=f"attachment://{filename}")
        try:
            files.append(discord.File(THUMBNAIL_FILE, filename=filename))
        except Exception:
            pass
    return embed, files


def _short_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s"
    m = int(seconds // 60)
    if m < 60:
        return f"{m}m"
    h = int(m // 60)
    return f"{h}h {m % 60}m"


def _account_state(state: State) -> str:
    if state.token and state.cookie:
        return "Connected"
    if state.token or state.cookie:
        return "Partially connected"
    return "Not connected"


class ChannelLockFailure(commands.CheckFailure):
    pass


class StreamController:
    def __init__(self, state, session, console, bot, user_id: int):
        self.state = state
        self.session = session
        self.console = console
        self.bot = bot
        self.user_id = user_id
        self.engine = None
        self.thread = None
        self._lock = asyncio.Lock()
        self.notify_channel_id = None

    def is_running(self):
        if self.engine is None: return False
        return self.engine.running or self.engine.is_breaking

    async def start(self, guild_id, voice_channel_id, notify_channel_id):
        async with self._lock:
            if self.is_running():
                return False, "You already have an active streaming session."
            if not (self.state.token and self.state.cookie):
                return False, "Your account is not connected. Use `/link` to set it up."
            if not self.state.viewer_tokens:
                return False, "No viewer accounts connected. Use `/link` to add at least one."
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: ensure_token(self.state, self.user_id, self.session.super_props)
                )
            except Exception as exc:
                self.console.err("auth", f"token fetch[{self.user_id}]: {exc}")
                return False, "Could not verify your account. Please re-link and try again."
            
            self.notify_channel_id = notify_channel_id
            
            engine = GatewayStreamEngine(
                self.state, self.user_id, self.session, self.console,
                on_stop=self._on_engine_stop,
            )
            self.engine = engine
            self.thread = threading.Thread(
                target=engine.start_stream,
                args=(guild_id, voice_channel_id, False), daemon=True,
            )
            self.thread.start()

            loop = asyncio.get_event_loop()
            started = await loop.run_in_executor(
                None, lambda: engine.stream_created.wait(timeout=30)
            )
            
            if started:
                return True, "Your account is now live."
            
            if not engine.running and not engine.is_breaking:
                self.engine = None
                return False, "Could not connect to Discord. Please try again."
            
            return True, "Connecting to Discord..."

    async def stop(self):
        async with self._lock:
            if not self.is_running():
                return False, "You don't have an active streaming session."
            self.engine.stop_stream()
            return True, "Ending your session..."

    def _on_engine_stop(self, engine):
        try:
            if not self.bot.loop or self.bot.loop.is_closed(): return
            asyncio.run_coroutine_threadsafe(self._notify_stop(engine), self.bot.loop)
        except Exception: pass

    async def _notify_stop(self, engine):
        try:
            if not self.notify_channel_id: return
            ch = self.bot.get_channel(self.notify_channel_id)
            if not ch: return
            
            el = engine.last_duration
            embed = discord.Embed(
                title="Session Ended",
                description=(
                    f"**Duration:** {_short_duration(el)}\n"
                    f"**Total streamed:** {self.state.total_stream_hours_claimed:.1f} hours\n"
                    f"**Sessions completed:** {self.state.total_stream_sessions}"
                ),
                color=EMBED_COLOR,
            )
            embed, files = _with_thumb(embed)
            kw = {"embed": embed}
            if files: kw["files"] = files
            await ch.send(**kw)
        except Exception as exc:
            self.console.warn("bot", f"notification[{self.user_id}]: {exc}")


class LinkModal(discord.ui.Modal, title="Connect Tokens"):
    main_token = discord.ui.TextInput(
        label="Streamer Token",
        placeholder="Leave empty to keep current",
        required=False, max_length=200,
    )
    cf_cookie = discord.ui.TextInput(
        label="Clearance Cookie",
        placeholder="Leave empty to keep current",
        required=False, max_length=500,
    )
    viewer_tokens = discord.ui.TextInput(
        label="Viewer Token",
        placeholder="Your Viewer Token",
        required=False, style=discord.TextStyle.paragraph, max_length=4000,
    )

    def __init__(self, bot, user_id: int):
        super().__init__()
        self.bot = bot
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        if LOCK_ID and interaction.channel_id != LOCK_ID:
            try:
                await interaction.response.send_message(
                    f"This bot can only be used in <#{LOCK_ID}>.", ephemeral=True,
                )
            except Exception: pass
            return

        await interaction.response.defer(ephemeral=True)
        state = self.bot.get_state(self.user_id)
        results = []

        new_main = (self.main_token.value or "").strip()
        if new_main:
            state.token = new_main
            state.fetched_at = 0
            try:
                ensure_token(state, self.user_id, self.bot.session.super_props)
                
                privacy_ok = enable_personalization(state.token, self.bot.session.super_props)
                if privacy_ok:
                    results.append(("Privacy", "Personalization Enabled"))
                else:
                    results.append(("Privacy", "Failed to auto-enable"))
                
                results.append(("Account", "Connected and verified"))
            except Exception as exc:
                self.bot.console.warn("link", f"main verify[{self.user_id}]: {exc}")
                results.append(("Account", "Saved, could not verify"))

        if (self.cf_cookie.value or "").strip():
            state.cookie = self.cf_cookie.value.strip()
            results.append(("Clearance", "Saved"))

        added = failed = 0
        if (self.viewer_tokens.value or "").strip():
            for tok in [t.strip() for t in self.viewer_tokens.value.strip().split("\n") if t.strip()]:
                if tok == state.token or tok in state.viewer_tokens: continue
                ok, _ = validate_viewer_token(tok, self.bot.session.super_props)
                if ok:
                    state.viewer_tokens.append(tok); added += 1
                else:
                    failed += 1
            if added or failed:
                label = f"{added} added" + (f", {failed} rejected" if failed else "")
                results.append(("Viewer accounts", label))

        state.save(self.user_id)

        embed = discord.Embed(title="Link Result", color=EMBED_COLOR)
        if not results:
            embed.description = "No changes were made."
        else:
            for n, v in results:
                embed.add_field(name=n, value=v, inline=True)
        embed.set_footer(text=f"Viewer accounts connected: {len(state.viewer_tokens)}")

        embed, files = _with_thumb(embed)
        try:
            kw = {"embed": embed, "ephemeral": True}
            if files: kw["files"] = files
            await interaction.followup.send(**kw)
        except Exception: pass


class ConfirmView(discord.ui.View):
    def __init__(self, on_confirm):
        super().__init__(timeout=45)
        self.on_confirm = on_confirm

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
    async def confirm(self, i: discord.Interaction, b: discord.ui.Button):
        self.stop(); await self.on_confirm(i)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, i: discord.Interaction, b: discord.ui.Button):
        self.stop()
        embed = discord.Embed(title="Cancelled", color=EMBED_COLOR)
        embed, files = _with_thumb(embed)
        kw = {"embed": embed, "view": None}
        if files: kw["files"] = files
        await i.response.edit_message(**kw)


class StreamBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        super().__init__(command_prefix=PREFIX, intents=intents, help_command=None)
        self.console = Console()
        self.session = Session.new()
        self._states: dict[int, State] = {}
        self._controllers: dict[int, StreamController] = {}
        self._lock = threading.RLock()

    def get_state(self, user_id: int) -> State:
        with self._lock:
            if user_id in self._states:
                return self._states[user_id]
        loaded = State.load(user_id)
        with self._lock:
            if user_id not in self._states:
                self._states[user_id] = loaded
            return self._states[user_id]

    def get_controller(self, user_id: int) -> StreamController:
        state = self.get_state(user_id)
        with self._lock:
            if user_id not in self._controllers:
                self._controllers[user_id] = StreamController(
                    state, self.session, self.console, self, user_id,
                )
            return self._controllers[user_id]

    def _lock_message(self) -> str:
        if not LOCK_ID: return ""
        try:
            ch = self.get_channel(LOCK_ID)
            if ch: return f"This bot only responds in {ch.mention}."
        except Exception: pass
        return "This bot can only be used in a specific channel."

    async def setup_hook(self):
        @self.check
        async def _prefix_channel_lock(ctx):
            if LOCK_ID and ctx.channel.id != LOCK_ID:
                raise ChannelLockFailure()
            return True

        async def _slash_channel_lock(interaction: discord.Interaction) -> bool:
            if not LOCK_ID or interaction.channel_id == LOCK_ID:
                return True
            try:
                await interaction.response.send_message(self._lock_message(), ephemeral=True)
            except Exception:
                try:
                    await interaction.followup.send(self._lock_message(), ephemeral=True)
                except Exception: pass
            return False
        self.tree.interaction_check = _slash_channel_lock

        _register_commands(self)
        if GUILD_ID:
            g = discord.Object(id=GUILD_ID)
            self.tree.copy_global_to(guild=g)
            try:
                await self.tree.sync(guild=g)
                self.console.ok("bot", "commands synced")
            except Exception as exc:
                self.console.err("bot", f"sync: {exc}")
        else:
            try:
                await self.tree.sync()
                self.console.ok("bot", "commands synced")
            except Exception as exc:
                self.console.err("bot", f"sync: {exc}")

    async def on_ready(self):
        self.console.ok("bot", f"logged in as {self.user}")
        self.console.info("mode · multi-session enabled")
        if GUILD_ID and CHANNEL_ID:
            self.console.ok("config", "stream target configured")
        else:
            self.console.warn("config", "stream target missing — /start will fail")
        if LOCK_ID:
            self.console.ok("lock", f"locked to channel {LOCK_ID}")
        else:
            self.console.info("lock · no channel lock configured")
        
        if THUMBNAIL_URL:
            self.console.ok("theme", "thumbnail loaded from URL")
        elif THUMBNAIL_FILE:
            self.console.ok("theme", f"thumbnail loaded from {os.path.basename(THUMBNAIL_FILE)}")
        else:
            self.console.info("theme · no thumbnail configured (optional)")
        try:
            await self.change_presence(activity=discord.Activity(
                type=discord.ActivityType.watching, name=f"{PREFIX}help"
            ))
        except Exception: pass

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.CommandNotFound):
            return
        if isinstance(error, ChannelLockFailure):
            return
        self.console.err("bot", f"command error: {error}")
        try:
            embed = discord.Embed(
                title="Something Went Wrong",
                description="Please try again. If the problem persists, contact the bot owner.",
                color=EMBED_COLOR,
            )
            embed, files = _with_thumb(embed)
            kw = {"embed": embed}
            if files: kw["files"] = files
            if ctx.interaction: kw["ephemeral"] = True
            try: await ctx.send(**kw)
            except Exception:
                kw.pop("ephemeral", None)
                await ctx.send(**kw)
        except Exception: pass


async def _respond(ctx, content=None, *, embed=None, view=None):
    embed, files = _with_thumb(embed)
    kw = {}
    if content is not None: kw["content"] = content
    if embed is not None: kw["embed"] = embed
    if view is not None: kw["view"] = view
    if files: kw["files"] = files
    if ctx.interaction: kw["ephemeral"] = True
    try: await ctx.send(**kw)
    except Exception:
        kw.pop("ephemeral", None)
        try: await ctx.send(**kw)
        except Exception: pass


async def _del(ctx):
    pass


async def _modal(ctx, modal):
    if ctx.interaction:
        try:
            await ctx.interaction.response.send_modal(modal); return True
        except Exception: return False
    try:
        await ctx.send_modal(modal); return True
    except Exception: return False


def _register_commands(bot: StreamBot):

    @bot.hybrid_command(name="link", description="Connect your account and viewer accounts")
    async def link_cmd(ctx):
        await _del(ctx)
        if not await _modal(ctx, LinkModal(bot, ctx.author.id)):
            await _respond(ctx, embed=discord.Embed(
                title="Link Unavailable",
                description="Could not open the connect dialog. Try the slash command.",
                color=EMBED_COLOR,
            ))

    @bot.hybrid_command(name="unlink", description="Disconnect your linked accounts")
    @app_commands.describe(scope="What to disconnect")
    @app_commands.choices(scope=[
        app_commands.Choice(name="Everything", value="all"),
        app_commands.Choice(name="Viewer accounts only", value="viewers"),
        app_commands.Choice(name="Main account only", value="main"),
    ])
    async def unlink_cmd(ctx, scope: app_commands.Choice[str] = None):
        await _del(ctx)
        chosen = scope.value if scope else "all"
        labels = {
            "all": "all your linked accounts",
            "viewers": "your viewer accounts",
            "main": "your main account",
        }
        if bot.get_controller(ctx.author.id).is_running():
            await _respond(ctx, embed=discord.Embed(
                title="Cannot Disconnect",
                description="Stop your stream with `/stop` before disconnecting accounts.",
                color=EMBED_COLOR,
            ))
            return
        user_id = ctx.author.id
        async def do(i: discord.Interaction):
            st = bot.get_state(user_id)
            if chosen == "viewers":
                st.viewer_tokens = []
            elif chosen == "main":
                st.token = ""; st.cookie = ""; st.analytics_token = ""; st.fetched_at = 0
            else:
                st.token = ""; st.cookie = ""; st.analytics_token = ""; st.fetched_at = 0
                st.viewer_tokens = []
            st.save(user_id)
            embed = discord.Embed(
                title="Disconnected",
                description=f"Removed: {labels[chosen]}",
                color=EMBED_COLOR,
            )
            embed, files = _with_thumb(embed)
            kw = {"embed": embed, "view": None}
            if files: kw["files"] = files
            await i.response.edit_message(**kw)
        await _respond(ctx, embed=discord.Embed(
            title="Confirm Disconnect",
            description=f"This will disconnect **{labels[chosen]}**. Continue?",
            color=EMBED_COLOR,
        ), view=ConfirmView(do))

    @bot.hybrid_command(name="start", description="Begin your streaming session")
    async def start_cmd(ctx):
        await _del(ctx)
        user_id = ctx.author.id

        if not GUILD_ID or not CHANNEL_ID:
            await _respond(ctx, embed=discord.Embed(
                title="Not Configured",
                description="Streaming is not set up. Contact the bot owner.",
                color=EMBED_COLOR,
            ))
            return

        if ctx.interaction: await ctx.defer(ephemeral=True)
        else: await ctx.defer()

        ctrl = bot.get_controller(user_id)
        ok, msg = await ctrl.start(str(GUILD_ID), CHANNEL_ID, ctx.channel.id)

        guild_display = "the server"
        channel_display = "the voice channel"
        try:
            g = bot.get_guild(GUILD_ID)
            if g: guild_display = g.name
            ch = bot.get_channel(int(CHANNEL_ID))
            if ch: channel_display = f"#{ch.name}"
        except Exception: pass

        e = discord.Embed(
            title="Stream Started" if ok else "Could Not Start",
            description=msg, color=EMBED_COLOR,
        )
        if ok:
            e.add_field(name="Server", value=guild_display, inline=True)
            e.add_field(name="Channel", value=channel_display, inline=True)
            e.add_field(name="Viewers", value=str(len(bot.get_state(user_id).viewer_tokens)), inline=True)
            e.set_footer(text="Use /stop to end your session · /status for details")
        await _respond(ctx, embed=e)

    @bot.hybrid_command(name="stop", description="End your streaming session")
    async def stop_cmd(ctx):
        await _del(ctx)
        ctrl = bot.get_controller(ctx.author.id)
        ok, msg = await ctrl.stop()
        await _respond(ctx, embed=discord.Embed(
            title="Session Ending" if ok else "Nothing to Stop",
            description=msg, color=EMBED_COLOR,
        ))

    @bot.hybrid_command(name="status", description="View your current session status")
    async def status_cmd(ctx):
        await _del(ctx)
        s = bot.get_state(ctx.author.id)
        ctrl = bot.get_controller(ctx.author.id)
        e = discord.Embed(title="Your Stream Status", color=EMBED_COLOR)
        running = ctrl.is_running()
        e.add_field(name="Session", value="Active" if running else "Idle", inline=True)
        if running and ctrl.engine:
            en = ctrl.engine
            el = time.time() - en.start_time if en.start_time else 0
            e.add_field(name="Duration", value=_short_duration(el), inline=True)
            e.add_field(name="Live", value="Yes" if en.stream_connected else ("Breaking" if en.is_breaking else "Starting"), inline=True)
            if en.viewer_manager:
                e.add_field(name="Viewers in Channel", value=en.viewer_manager.status_summary(), inline=False)
        e.add_field(name="Account", value=_account_state(s), inline=True)
        e.add_field(
            name="Viewer Accounts",
            value=str(len(s.viewer_tokens)) if s.viewer_tokens else "None",
            inline=True,
        )
        e.add_field(name="Total Streamed", value=f"{s.total_stream_hours_claimed:.1f}h", inline=True)
        e.add_field(name="Sessions Completed", value=str(s.total_stream_sessions), inline=True)

        if CHANNEL_ID:
            try:
                ch = bot.get_channel(int(CHANNEL_ID))
                if ch: e.add_field(name="Target Channel", value=ch.mention, inline=True)
            except Exception: pass

        await _respond(ctx, embed=e)

    @bot.hybrid_command(name="tiers", description="View your streaming badge tier progress")
    async def tiers_cmd(ctx):
        await _del(ctx)
        s = bot.get_state(ctx.author.id)
        hours = s.total_stream_hours_claimed
        
        e = discord.Embed(
            title="Streaming Badge Tiers",
            description=f"**Total Streamed:** {hours:.1f} hours\n",
            color=EMBED_COLOR,
        )
        
        tier_text = ""
        for tier_num, name, req_hours in STREAM_TIERS:
            if hours >= req_hours:
                tier_text += f"✓ **Tier {tier_num}** · {name} ({req_hours}h)\n"
            else:
                remaining = req_hours - hours
                tier_text += f"□ **Tier {tier_num}** · {name} ({req_hours}h) — {remaining:.1f}h left\n"
                
        e.add_field(name="Progress", value=tier_text, inline=False)
        await _respond(ctx, embed=e)

    @bot.hybrid_command(name="help", description="How to use this bot")
    async def help_cmd(ctx):
        await _del(ctx)
        e = discord.Embed(
            title="Nox Streamer",
            description=f"Prefix: `{PREFIX}`",
            color=EMBED_COLOR,
        )
        e.add_field(
            name="Getting Started",
            value=(
                "**1.** Use `/link` to connect your account and at least one viewer\n"
                "**2.** Use `/start` to begin streaming\n"
                "**3.** Use `/stop` when you're finished"
            ),
            inline=False,
        )
        e.add_field(
            name="Before You Start",
            value=(
                "• At least one viewer account must be connected\n"
                "• Your credentials stay private and are never exposed"
            ),
            inline=False,
        )
        e.set_footer(text="Nox Streamer | By okidaa")
        await _respond(ctx, embed=e)


def main():
    if not BOT_TOKEN:
        print("Bot token is not configured. The bot cannot start.")
        return
    bot = StreamBot()
    try: bot.run(BOT_TOKEN)
    except KeyboardInterrupt: print("Shutting down...")


if __name__ == "__main__":
    main()