// src/index.ts
import { Worker } from "@notionhq/workers"
import * as Schema from "@notionhq/workers/schema"
import * as Builder from "@notionhq/workers/builder"
import iconv from "iconv-lite"

const worker = new Worker()
export default worker

const sina = worker.pacer("sina", { allowedRequests: 10, intervalMs: 1000 })

// ───────────────────────── Managed DBs ─────────────────────────
const quotes = worker.database("quotes", {
  type: "managed",
  initialTitle: "📈 A 股日线行情",
  primaryKeyProperty: "代码-日期",
  schema: {
    properties: {
      "代码-日期": Schema.title(),
      "代码":     Schema.richText(),
      "名称":     Schema.richText(),
      "日期":     Schema.date(),
      "开盘":     Schema.number(),
      "收盘":     Schema.number(),
      "最高":     Schema.number(),
      "最低":     Schema.number(),
      "成交量":   Schema.number(),
      "成交额":   Schema.number(),
      "数据源":   Schema.select([
        { name: "sina",    color: "blue"  },
        { name: "tushare", color: "green" },
      ]),
    },
  },
})

const fundNav = worker.database("fundNav", {
  type: "managed",
  initialTitle: "💹 基金净值",
  primaryKeyProperty: "代码-日期",
  schema: {
    properties: {
      "代码-日期": Schema.title(),
      "代码":     Schema.richText(),
      "名称":     Schema.richText(),
      "净值日期": Schema.date(),
      "累计净值": Schema.number(),
      "日涨幅":   Schema.number(),
      "数据源":   Schema.select([{ name: "sina", color: "blue" }]),
    },
  },
})

// ───────────────────────── 共享工具 ─────────────────────────
type TargetType = "stock" | "etf" | "lof" | "open_fund"
type Target = { code: string; type: TargetType }

async function sinaFetch(codes: string[]): Promise<Record<string, string>> {
  if (codes.length === 0) return {}
  await sina.wait()
  const url = `https://hq.sinajs.cn/list=${codes.join(",")}`
  const res = await fetch(url, { headers: { Referer: "https://finance.sina.com.cn" } })
  const buf = Buffer.from(await res.arrayBuffer())
  const text = iconv.decode(buf, "gb18030")
  const out: Record<string, string> = {}
  for (const line of text.split("\n")) {
    const m = line.match(/hq_str_(\w+)="([^"]*)"/)
    if (m) out[m[1]] = m[2]
  }
  return out
}

function chunk<T>(arr: T[], n: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n))
  return out
}

function isTradingDay(d: Date): boolean {
  const w = d.getDay()
  return w !== 0 && w !== 6
}

async function loadWatchlist(
  notion: import("@notionhq/client").Client,
): Promise<Target[]> {
  const dsId = process.env.WATCHLIST_DS_ID
  if (!dsId) throw new Error("WATCHLIST_DS_ID is not configured")

  const out: Target[] = []
  let cursor: string | undefined
  do {
    const resp = await notion.dataSources.query({
      data_source_id: dsId,
      filter: { property: "启用", checkbox: { equals: true } },
      start_cursor: cursor,
    })
    for (const page of resp.results) {
      const props = (page as any).properties
      const code = props["代码"]?.title?.[0]?.plain_text?.trim()
      const type = props["类型"]?.select?.name as TargetType | undefined
      if (!code || !type) continue
      out.push({ code, type })
    }
    cursor = resp.has_more ? resp.next_cursor ?? undefined : undefined
  } while (cursor)
  return out
}

// 单 sync 单 target：不再带 targetDatabaseKey
function buildStockUpsert(code: string, payload: string, date: string) {
  const f = payload.split(",")
  if (f.length < 10 || !f[1] || f[1] === "0.000") return null
  const pk = `${code}-${date}`
  return {
    type: "upsert" as const,
    key: pk,
    properties: {
      "代码-日期": Builder.title(pk),
      "代码":     Builder.richText(code),
      "名称":     Builder.richText(f[0]),
      "日期":     Builder.date(date),
      "开盘":     Builder.number(parseFloat(f[1])),
      "收盘":     Builder.number(parseFloat(f[3])),
      "最高":     Builder.number(parseFloat(f[4])),
      "最低":     Builder.number(parseFloat(f[5])),
      "成交量":   Builder.number(parseFloat(f[8])),
      "成交额":   Builder.number(parseFloat(f[9])),
      "数据源":   Builder.select("sina"),
    },
    upstreamUpdatedAt: `${date}T15:30:00+08:00`,
  }
}

function buildFundUpsert(rawKey: string, payload: string) {
  const code = rawKey.replace(/^fu_/, "")
  const f = payload.split(",")
  if (f.length < 10) return null

  const name    = f[0]
  const accNav  = parseFloat(f[2])
  const navDate = f[7]
  const pctChg  = parseFloat(f[9])
  if (!navDate || Number.isNaN(accNav)) return null

  const pk = `${code}-${navDate}`
  return {
    type: "upsert" as const,
    key: pk,
    properties: {
      "代码-日期": Builder.title(pk),
      "代码":     Builder.richText(code),
      "名称":     Builder.richText(name),
      "净值日期": Builder.date(navDate),
      "累计净值": Builder.number(accNav),
      "日涨幅":   Builder.number(Number.isNaN(pctChg) ? 0 : Number(pctChg.toFixed(4))),
      "数据源":   Builder.select("sina"),
    },
    upstreamUpdatedAt: `${navDate}T20:00:00+08:00`,
  }
}

// ───────────────────────── Sync #1: 股票 / ETF / LOF ─────────────────────────
worker.sync("dailyQuote", {
  database: quotes,
  mode: "incremental",
  schedule: "1d",

  async execute(_state, { notion }) {
    const now = new Date()
    // if (!isTradingDay(now)) return { changes: [], hasMore: false }
    const date = now.toISOString().slice(0, 10)

    const targets = await loadWatchlist(notion)
    const codes = targets.filter(t => t.type !== "open_fund").map(t => t.code)

    const map: Record<string, string> = {}
    await Promise.all(
      chunk(codes, 80).map(async batch =>
        Object.assign(map, await sinaFetch(batch))),
    )

    const changes = codes
      .map(code => {
        const p = map[code]
        return p ? buildStockUpsert(code, p, date) : null
      })
      .filter(<T,>(x: T | null): x is T => x !== null)

    return { changes, hasMore: false }
  },
})

// ───────────────────────── Sync #2: 场外基金净值 ─────────────────────────
worker.sync("dailyFundNav", {
  database: fundNav,
  mode: "incremental",
  schedule: "1d",

  async execute(_state, { notion }) {
    const targets = await loadWatchlist(notion)
    const keys = targets
      .filter(t => t.type === "open_fund")
      .map(t => (t.code.startsWith("fu_") ? t.code : `fu_${t.code}`))

    const map: Record<string, string> = {}
    await Promise.all(
      chunk(keys, 80).map(async batch =>
        Object.assign(map, await sinaFetch(batch))),
    )

    const changes = keys
      .map(rawKey => {
        const p = map[rawKey]
        return p ? buildFundUpsert(rawKey, p) : null
      })
      .filter(<T,>(x: T | null): x is T => x !== null)

    return { changes, hasMore: false }
  },
})