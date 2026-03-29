from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time


def resp_array(*values: str) -> bytes:
    parts = [f"*{len(values)}\r\n".encode()]
    for value in values:
        encoded = value.encode()
        parts.append(f"${len(encoded)}\r\n".encode())
        parts.append(encoded + b"\r\n")
    return b"".join(parts)


async def perform_request(host: str, port: int, index: int) -> float:
    reader, writer = await asyncio.open_connection(host, port)
    started = time.perf_counter()
    writer.write(resp_array("SET", f"key:{index}", str(index)))
    await writer.drain()
    await reader.readuntil(b"\r\n")
    writer.write(resp_array("GET", f"key:{index}"))
    await writer.drain()
    header = await reader.readuntil(b"\r\n")
    if header != b"$-1\r\n":
        size = int(header[1:-2])
        await reader.readexactly(size + 2)
    elapsed_ms = (time.perf_counter() - started) * 1000
    writer.close()
    await writer.wait_closed()
    return elapsed_ms


async def perform_pipelined_request(host: str, port: int, index: int, pipeline_depth: int) -> float:
    reader, writer = await asyncio.open_connection(host, port)
    started = time.perf_counter()
    commands: list[tuple[str, ...]] = []
    for offset in range(pipeline_depth):
        current = index * pipeline_depth + offset
        commands.append(("SET", f"pipe:{current}", str(current)))
        commands.append(("GET", f"pipe:{current}"))
    for command in commands:
        writer.write(resp_array(*command))
    await writer.drain()
    for command in commands:
        header = await reader.readuntil(b"\r\n")
        if command[0] == "GET":
            if header != b"$-1\r\n":
                size = int(header[1:-2])
                await reader.readexactly(size + 2)
    elapsed_ms = (time.perf_counter() - started) * 1000
    writer.close()
    await writer.wait_closed()
    return elapsed_ms


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--p95-threshold-ms", type=float, default=100.0)
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--pipeline-depth", type=int, default=1)
    args = parser.parse_args()

    semaphore = asyncio.Semaphore(args.concurrency)

    async def runner(index: int) -> float:
        async with semaphore:
            if args.pipeline_depth > 1:
                return await perform_pipelined_request(args.host, args.port, index, args.pipeline_depth)
            return await perform_request(args.host, args.port, index)

    latencies = await asyncio.gather(*(runner(i) for i in range(args.requests)))
    p50 = statistics.median(latencies)
    sorted_latencies = sorted(latencies)
    p95_index = max(int(len(sorted_latencies) * 0.95) - 1, 0)
    p99_index = max(int(len(sorted_latencies) * 0.99) - 1, 0)
    p95 = sorted_latencies[p95_index]
    p99 = sorted_latencies[p99_index]
    report = {
        "requests": args.requests,
        "concurrency": args.concurrency,
        "pipeline_depth": args.pipeline_depth,
        "mean_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "p99_ms": round(p99, 2),
    }
    print(
        f"requests={args.requests} concurrency={args.concurrency} pipeline_depth={args.pipeline_depth} "
        f"p50_ms={p50:.2f} p95_ms={p95:.2f} p99_ms={p99:.2f}"
    )
    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)
    if p95 > args.p95_threshold_ms:
        raise SystemExit(
            f"p95 latency {p95:.2f}ms exceeded threshold {args.p95_threshold_ms:.2f}ms"
        )


if __name__ == "__main__":
    asyncio.run(main())
