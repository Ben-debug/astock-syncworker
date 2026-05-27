// src/index.ts
import { Worker } from "@notionhq/workers"
import * as Schema from "@notionhq/workers/schema"
import * as Builder from "@notionhq/workers/builder"
import iconv from "iconv-lite"

const worker = new Worker()
export default worker

const sina      = worker.pacer("sina",      { allowedRequests: 10, intervalMs: 1000 })
const eastmoney = worker.pacer("eastmoney", { allowedRequests: 3,  intervalMs: 1000 })

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
      "单位净值": Schema.number(),  // 新增：基金公司当日公布的标准净值
      "累计净值": Schema.number(),
      "日涨幅":   Schema.number(),
      "数据源":   Schema.select([
        { name: "sina",      color: "blue"   },
        { name: "eastmoney", color: "orange" },  // 新增
      ]),
    },
  },
})

// ───────────────────────── 类型 / 工具 ─────────────────────────
type TargetType = "stock" | "etf" | "lof" | "open_fund"
type Target = { code: string; type: TargetType; name: string }

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
      const name = props["名称"]?.rich_text?.[0]?.plain_text?.trim() ?? ""
      if (!code || !type) continue
      out.push({ code, type, name })
    }
    cursor = resp.has_more ? resp.next_cursor ?? undefined : undefined
  } while (cursor)
  return out
}

// ───────────────────────── 数据源 A：新浪（股票 / ETF / LOF）─────────────────────────
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

function buildStockUpsert(code: string, payload: string) {
  const f = payload.split(",")
  // Sina 股票接口字段：0=名称 1=开盘 2=昨收 3=当前价(收盘) 4=最高 5=最低
  //                  6=买一 7=卖一 8=成交量 9=成交额 ... 30=日期 31=时间
  if (f.length < 32) return null

  const current = parseFloat(f[3])
  if (!Number.isFinite(current) || current <= 0) return null  // 停牌/异常

  const date = f[30]
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null

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
    upstreamUpdatedAt: `${date}T${f[31] || "15:30:00"}+08:00`,
  }
}

// ───────────────────────── 数据源 B：天天基金 lsjz（场外基金）─────────────────────────
type LsjzRow = {
  FSRQ:  string   // 净值日期 yyyy-mm-dd
  DWJZ:  string   // 单位净值
  LJJZ:  string   // 累计净值
  JZZZL: string   // 日涨跌率（%，字符串）
  SGZT:  string   // 申购状态
  SHZT:  string   // 赎回状态
}

async function fetchLsjz(code: string, pageSize: number = 30): Promise<LsjzRow[]> {
  await eastmoney.wait()
  const url = `https://api.fund.eastmoney.com/f10/lsjz?fundCode=${code}&pageIndex=1&pageSize=${pageSize}`
  const res = await fetch(url, {
    headers: {
      "Referer":    "https://fundf10.eastmoney.com/",
      "User-Agent": "Mozilla/5.0",
    },
  })
  if (!res.ok) {
    throw new Error(`lsjz HTTP ${res.status} for ${code}`)
  }
  const json: any = await res.json()
  if (json.ErrCode !== undefined && json.ErrCode !== 0) {
    throw new Error(`lsjz ErrCode=${json.ErrCode} ${json.ErrMsg ?? ""} for ${code}`)
  }
  return json.Data?.LSJZList ?? []
}

function buildFundUpsertFromLsjz(code: string, name: string, row: LsjzRow) {
  const navDate = row.FSRQ
  if (!navDate || !/^\d{4}-\d{2}-\d{2}$/.test(navDate)) return null

  const unitNav = parseFloat(row.DWJZ)
  if (!Number.isFinite(unitNav)) return null

  const accNav  = parseFloat(row.LJJZ)
  const pctChg  = parseFloat(row.JZZZL)

  const pk = `${code}-${navDate}`
  return {
    type: "upsert" as const,
    key: pk,
    properties: {
      "代码-日期": Builder.title(pk),
      "代码":     Builder.richText(code),
      "名称":     Builder.richText(name),
      "净值日期": Builder.date(navDate),
      "单位净值": Builder.number(unitNav),
      "累计净值": Builder.number(Number.isFinite(accNav) ? accNav : 0),
      "日涨幅":   Builder.number(Number.isFinite(pctChg) ? Number(pctChg.toFixed(4)) : 0),
      "数据源":   Builder.select("eastmoney"),
    },
    upstreamUpdatedAt: `${navDate}T20:00:00+08:00`,
  }
}

// ───────────────────────── Sync #1: 股票 / ETF / LOF（新浪）─────────────────────────
worker.sync("dailyQuote", {
  database: quotes,
  mode: "incremental",
  schedule: "1d",

  async execute(_state, { notion }) {
    // const now = new Date()
    // if (!isTradingDay(now)) return { changes: [], hasMore: false }

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
        return p ? buildStockUpsert(code, p) : null
      })
      .filter(<T,>(x: T | null): x is T => x !== null)

    return { changes, hasMore: false }
  },
})

// ───────────────────────── Sync #2: 场外基金净值（天天基金 lsjz）─────────────────────────
// 每次拉最近 30 天 → 自动补齐过去 1 个月任何漏数 + 修正旧 sina 偏差行
worker.sync("dailyFundNav", {
  database: fundNav,
  mode: "incremental",
  schedule: "1d",

  async execute(_state, { notion }) {
    const targets = await loadWatchlist(notion)
    const funds = targets.filter(t => t.type === "open_fund")

    const allChanges: NonNullable<ReturnType<typeof buildFundUpsertFromLsjz>>[] = []
    const errors: string[] = []

    // 串行：lsjz 对突发不友好，叠加 pacer 3 req/s 限速
    for (const t of funds) {
      const code = t.code.replace(/^fu_/, "")
      try {
        const rows = await fetchLsjz(code, 30)
        for (const row of rows) {
          const change = buildFundUpsertFromLsjz(code, t.name, row)
          if (change) allChanges.push(change)
        }
      } catch (e: any) {
        errors.push(`${code}: ${e?.message ?? String(e)}`)
      }
    }

    if (errors.length > 0) {
      console.error("[dailyFundNav] errors:", errors)
    }

    return { changes: allChanges, hasMore: false }
  },
})