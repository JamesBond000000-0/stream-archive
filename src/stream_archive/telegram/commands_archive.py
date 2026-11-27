"""Telegram /archive command: archive a channel's VODs to YouTube.

The handler is a thin shell over the archive service. The service
owns the run state, so the reply here is only the start result or,
without an argument, the current run's status.
"""

from typing import Any


class ArchiveCommands:
    """The /archive command handler."""

    _archive_service: Any

    async def handle_archive(self, args: list[str]) -> str:
        """Start an archive run, or report the current one."""
        service = self._archive_service
        if service is None:
            return "Archive mode is not wired up in this run."
        if not args:
            return f"{service.status()}\nUsage: /archive <twitch username>"
        return str(await service.start(args[0]))
