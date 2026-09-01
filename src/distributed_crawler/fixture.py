from __future__ import annotations

import argparse
import asyncio
import logging

from aiohttp import web


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic local crawler fixture")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--pages", type=int, default=400)
    parser.add_argument("--fanout", type=int, default=16)
    parser.add_argument("--latency-ms", type=float, default=15)
    return parser.parse_args()


def make_app(pages: int, fanout: int, latency_ms: float) -> web.Application:
    app = web.Application()

    async def robots(_: web.Request) -> web.Response:
        return web.Response(text="User-agent: *\nDisallow: /blocked\n")

    async def blocked(_: web.Request) -> web.Response:
        return web.Response(status=418, text="crawler should not fetch this")

    async def page(request: web.Request) -> web.Response:
        if request.path == "/slow":
            await asyncio.sleep(2)
        elif latency_ms > 0:
            await asyncio.sleep(latency_ms / 1000)
        try:
            index = int(request.query.get("n", "0"))
        except ValueError:
            index = 0
        links = ["<!doctype html><title>fixture</title><a href='/blocked'>blocked</a>"]
        for step in range(1, fanout + 1):
            next_index = index + step
            if next_index < pages:
                links.append(f"<a href='/?n={next_index}'>page {next_index}</a>")
        return web.Response(text="".join(links), content_type="text/html")

    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/blocked", blocked)
    app.router.add_get("/{tail:.*}", page)
    return app


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    logging.info("fixture on http://%s:%d with %d pages", args.host, args.port, args.pages)
    web.run_app(
        make_app(args.pages, args.fanout, args.latency_ms),
        host=args.host,
        port=args.port,
        print=None,
    )


if __name__ == "__main__":
    main()

