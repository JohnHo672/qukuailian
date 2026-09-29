import { createHash } from "node:crypto";
import { readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import ts from "typescript";
import { ProxyAgent } from "undici";

const SRC = path.resolve("src");
const OUT = path.resolve("src/i18n/generated.ts");
const ATTRS = new Set(["title", "placeholder", "aria-label", "alt"]);
const PROPS = new Set([
  "label", "title", "description", "message", "emptyText", "helpText",
  "confirmText", "cancelText", "successMessage", "errorMessage",
]);
const proxyUrl = process.env.HTTPS_PROXY || process.env.HTTP_PROXY;
const dispatcher = proxyUrl ? new ProxyAgent(proxyUrl) : undefined;

const overrides = {
  "Buy": "买入", "Sell": "卖出", "Long": "做多", "Short": "做空",
  "Create Order": "创建订单", "Cancel Order": "撤销订单",
  "Stop Bot": "停止机器人", "Deploy Bot": "部署机器人",
  "Reduce Only": "仅减仓", "Position Side": "持仓方向",
  "Open": "开仓", "Close": "平仓", "Active": "运行中",
  "Pending": "待处理", "Failed": "失败", "Error": "错误",
  "Delete": "删除", "Confirm": "确认", "Cancel": "取消",
  "Portfolio": "资产组合", "Trade": "交易", "Bots": "机器人",
  "Agents": "智能助手", "Routines": "例程", "Settings": "设置",
  "Amount": "数量", "Price": "价格", "Leverage": "杠杆",
  "Stop Loss": "止损", "Take Profit": "止盈", "Side": "方向",
  "Market": "市价", "Limit": "限价", "Orders": "订单",
  "Positions": "持仓", "Balance": "余额", "Available balance": "可用余额",
  "Unrealized PnL": "未实现盈亏", "Realized PnL": "已实现盈亏",
  "Max Drawdown": "最大回撤", "Risk State": "风险状态",
};

async function files(dir) {
  const out = [];
  for (const ent of await readdir(dir, { withFileTypes: true })) {
    const full = path.join(dir, ent.name);
    if (ent.isDirectory()) out.push(...await files(full));
    else if (/\.tsx?$/.test(ent.name) && !/\.(test|spec)\./.test(ent.name) && path.resolve(full) !== OUT) out.push(full);
  }
  return out;
}

function clean(value) {
  return value.replace(/&amp;/g, "&").replace(/&apos;/g, "'")
    .replace(/\s+/g, " ").trim();
}

function plausible(value) {
  if (value.length < 2 || value.length > 180 || !/[A-Za-z]/.test(value)) return false;
  if (/^(https?:|wss?:|[./@]|--|var\(|rgba?\(|#[0-9a-f]|[A-Za-z]:\\)/i.test(value)) return false;
  if (/^[A-Z0-9_]+$/.test(value) && value.includes("_")) return false;
  if (/^[\w-]+(?:\s+[\w:[\]/().%-]+){4,}$/.test(value) && /(?:flex|text-|bg-|border|px-|py-|w-|h-)/.test(value)) return false;
  if (/[{}<>`]|\b(?:className|querySelector|Promise|Record|HTMLElement|document\.)\b/.test(value)) return false;
  if (/\.(?:tsx?|jsx?|py|ya?ml|css|json)$/.test(value)) return false;
  return true;
}

function inJsx(node) {
  for (let p = node.parent; p; p = p.parent) {
    if (ts.isJsxExpression(p) || ts.isJsxElement(p) || ts.isJsxSelfClosingElement(p)) return true;
    if (ts.isStatement(p)) return false;
  }
  return false;
}

function enclosingJsxAttribute(node) {
  for (let p = node.parent; p; p = p.parent) {
    if (ts.isJsxAttribute(p)) return p;
    if (ts.isJsxElement(p) || ts.isJsxSelfClosingElement(p) || ts.isStatement(p)) return undefined;
  }
  return undefined;
}

function collect(source, fileName, found) {
  const sf = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const add = (raw) => {
    const value = clean(raw);
    if (plausible(value)) found.add(value);
  };
  function visit(node) {
    if (ts.isJsxText(node)) add(node.text);
    if (ts.isJsxAttribute(node) && ATTRS.has(node.name.text) && node.initializer && ts.isStringLiteral(node.initializer)) add(node.initializer.text);
    if ((ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) && inJsx(node)) {
      const attribute = enclosingJsxAttribute(node);
      const attributeName = attribute && ts.isIdentifier(attribute.name) ? attribute.name.text : "";
      if (!attribute || ATTRS.has(attributeName) || PROPS.has(attributeName)) add(node.text);
    }
    if (ts.isPropertyAssignment(node) && ts.isIdentifier(node.name) && PROPS.has(node.name.text)) {
      if (ts.isStringLiteral(node.initializer) || ts.isNoSubstitutionTemplateLiteral(node.initializer)) add(node.initializer.text);
    }
    ts.forEachChild(node, visit);
  }
  visit(sf);
}

async function translateBatch(batch) {
  const query = batch.map(({ i, en }) => `[[[${i}]]] ${en}`).join("\n");
  const url = new URL("https://api.mymemory.translated.net/get");
  url.search = new URLSearchParams({ q: query, langpair: "en|zh-CN" });
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      const res = await fetch(url, { dispatcher, signal: AbortSignal.timeout(30000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      const translated = String(body?.responseData?.translatedText ?? "");
      const parsed = new Map();
      const re = /\[\[\[(\d+)\]\]\]\s*([\s\S]*?)(?=\n?\[\[\[\d+\]\]\]|$)/g;
      for (const match of translated.matchAll(re)) parsed.set(Number(match[1]), match[2].trim());
      if (parsed.size !== batch.length) throw new Error(`batch parse ${parsed.size}/${batch.length}`);
      return parsed;
    } catch (error) {
      if (attempt === 7) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1200 * (attempt + 1)));
    }
  }
}

const found = new Set(Object.keys(overrides));
for (const file of await files(SRC)) collect(await readFile(file, "utf8"), file, found);
const sourceTexts = [...found].sort((a, b) => a.localeCompare(b));
if (process.env.CONDOR_I18N_EXTRACT_ONLY === "1") {
  await writeFile(path.resolve("scripts/i18n-source.json"), JSON.stringify(sourceTexts, null, 2), "utf8");
  console.log(`wrote ${sourceTexts.length} source strings`);
  process.exit(0);
}
const results = new Array(sourceTexts.length);
const pending = [];
for (let i = 0; i < sourceTexts.length; i++) {
  if (overrides[sourceTexts[i]]) results[i] = overrides[sourceTexts[i]];
  else pending.push({ i, en: sourceTexts[i] });
}
const batches = [];
let batch = [];
let size = 0;
for (const item of pending) {
  const itemSize = item.en.length + 18;
  if (batch.length && size + itemSize > 430) {
    batches.push(batch);
    batch = [];
    size = 0;
  }
  batch.push(item);
  size += itemSize;
}
if (batch.length) batches.push(batch);

let batchCursor = 0;
async function worker() {
  while (batchCursor < batches.length) {
    const current = batchCursor++;
    const translated = await translateBatch(batches[current]);
    for (const [i, value] of translated) results[i] = value;
    if ((current + 1) % 25 === 0) process.stdout.write(`translated ${current + 1}/${batches.length} batches\n`);
  }
}
await Promise.all(Array.from({ length: 2 }, worker));

const entries = sourceTexts.map((en, i) => {
  const slug = en.toLowerCase().replace(/[^a-z0-9]+/g, ".").replace(/^\.|\.$/g, "").slice(0, 42) || "text";
  const hash = createHash("sha1").update(en).digest("hex").slice(0, 8);
  return { id: `generated.${slug}.${hash}`, en, zh: results[i] };
});
const contents = `// Generated by scripts/generate-i18n-catalog.mjs.\n// Review safety-critical terms in overrides before regeneration.\nexport const generatedEntries = ${JSON.stringify(entries, null, 2)} as const;\n`;
await writeFile(OUT, contents, "utf8");
console.log(`wrote ${entries.length} entries to ${path.relative(process.cwd(), OUT)}`);
