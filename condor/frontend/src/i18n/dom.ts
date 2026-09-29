import i18n, { chineseToEnglish, curatedEnglishToChinese, englishToChinese } from "./index";

const textSources = new WeakMap<Text, string>();
const attributeSources = new WeakMap<Element, Map<string, string>>();
const ATTRIBUTES = ["title", "placeholder", "aria-label", "alt"] as const;
const SKIP = "code, pre, textarea, script, style, [contenteditable='true'], .cm-editor";

const patterns: Array<[RegExp, (...parts: string[]) => string]> = [
  [/^(\d+) bots?$/i, (n) => `${n} 个机器人`],
  [/^(\d+) orders?$/i, (n) => `${n} 个订单`],
  [/^(\d+) positions?$/i, (n) => `${n} 个持仓`],
  [/^(\d+) trades?$/i, (n) => `${n} 笔成交`],
  [/^(\d+) executors?$/i, (n) => `${n} 个执行器`],
  [/^(\d+) sessions?$/i, (n) => `${n} 个会话`],
  [/^(\d+) assets? · (\d+) connectors?$/i, (assets, connectors) => `${assets} 项资产 · ${connectors} 个连接器`],
  [/^(\d+) controllers? · vol (.+)$/i, (controllers, volume) => `${controllers} 个控制器 · 成交量 ${volume}`],
  [/^(\d+) bots?, (\d+) controllers?$/i, (bots, controllers) => `${bots} 个机器人，${controllers} 个控制器`],
  [/^(\d+) controllers? total$/i, (n) => `共 ${n} 个控制器`],
  [/^(\d+) (controllers?|executors?) aggregated$/i, (n, kind) => `${n} 个${kind.toLowerCase().startsWith("controller") ? "控制器" : "执行器"}汇总`],
  [/^(\d+) active$/i, (n) => `${n} 个活动项`],
  [/^(\d+) in (.+) · vol (.+)$/i, (n, range, volume) => `${range} 内 ${n} 个 · 成交量 ${volume}`],
  [/^(\d+) total · (\d+) sessions?$/i, (total, sessions) => `共 ${total} 个 · ${sessions} 个会话`],
  [/^Executors \($/i, () => "执行器 ("],
  [/^Positions \($/i, () => "持仓 ("],
  [/^(\d+) seconds?$/i, (n) => `${n} 秒`],
  [/^(\d+) unread notification\(s\)$/i, (n) => `${n} 条未读通知`],
  [/^(\d+)m ago$/i, (n) => `${n} 分钟前`],
  [/^(\d+)h ago$/i, (n) => `${n} 小时前`],
  [/^(\d+)d ago$/i, (n) => `${n} 天前`],
  [/^(\d+) credentials? configured$/i, (n) => `已配置 ${n} 组 API 密钥`],
  [/^(\d+) credentials?, (\d+) wallets? configured$/i, (credentials, wallets) => `已配置 ${credentials} 组 API 密钥和 ${wallets} 个钱包`],
  [/^Select spot Exchange$/i, () => "选择现货交易所"],
  [/^Select perpetual Exchange$/i, () => "选择永续合约交易所"],
  [/^No spot connectors available\.$/i, () => "暂无可用的现货连接器。"],
  [/^No perpetual connectors available\.$/i, () => "暂无可用的永续合约连接器。"],
  [/^New chat with (.+)$/i, (name) => `与 ${name} 新建对话`],
  [/^Share with (.+)$/i, (name) => `与 ${name} 共享`],
  [/^Record voice message (.+)$/i, (shortcut) => `录制语音消息 ${shortcut}`],
  [/^Star (.+)$/i, (name) => `收藏 ${name}`],
  [/^Search (.+) markets$/i, (name) => `搜索 ${name} 市场`],
  [/^View only — no API keys for (.+)$/i, (name) => `只读模式 — 未配置 ${name} API 密钥`],
  [/^(.+) · local$/i, (name) => `${translateCore(name)} · 本地`],
  [/^Open (.+) — what it knows and what it runs$/i, (name) => `打开 ${name} — 查看其知识和运行内容`],
  [/^Failed to load (.+)$/i, (name) => `加载${translateCore(name)}失败`],
  [/^Delete (.+)\?$/i, (name) => `确定删除${translateCore(name)}吗？`],
  [/^Back to (.+)$/i, (name) => `返回${translateCore(name)}`],
  [/^Add (.+)$/i, (name) => `添加${translateCore(name)}`],
  [/^Select (.+)$/i, (name) => `选择${translateCore(name)}`],
  [/^Showing (\d+) of (\d+)$/i, (shown, total) => `显示 ${shown} / ${total}`],
];

function translateCore(value: string) {
  const trimmed = value.trim();
  const curated = curatedEnglishToChinese.get(trimmed.toLocaleLowerCase("en"));
  if (curated) return curated;
  for (const [pattern, make] of patterns) {
    const match = trimmed.match(pattern);
    if (match) return make(...match.slice(1));
  }
  const exact = englishToChinese.get(trimmed);
  if (exact) return exact;
  return trimmed;
}

export function translateTextForDisplay(value: string) {
  return i18n.language === "en" ? value : translateCore(value);
}

function withWhitespace(source: string, translated: string) {
  const leading = source.match(/^\s*/)?.[0] ?? "";
  const trailing = source.match(/\s*$/)?.[0] ?? "";
  return `${leading}${translated}${trailing}`;
}

function excluded(node: Node) {
  const parent = node.nodeType === Node.ELEMENT_NODE ? node as Element : node.parentElement;
  return Boolean(parent?.closest(SKIP));
}

function localizeText(node: Text) {
  if (excluded(node)) return;
  const current = node.data;
  const trimmed = current.trim();
  if (!trimmed) return;
  const language = i18n.language === "en" ? "en" : "zh-CN";
  if (language === "zh-CN" && trimmed === "s") {
    const previous = node.previousSibling?.textContent?.trim();
    if (["机器人", "控制器", "执行器", "持仓"].includes(previous ?? "")) node.data = "";
    return;
  }
  let source = textSources.get(node);
  if (language === "en") {
    if (source && current !== withWhitespace(current, source)) node.data = withWhitespace(current, source);
    return;
  }
  if (source) {
    const expected = translateCore(source);
    if (trimmed === expected) return;
    if (trimmed !== source && !englishToChinese.has(trimmed)) source = undefined;
  }
  if (!source) {
    if (chineseToEnglish.has(trimmed) || !/[A-Za-z]/.test(trimmed)) return;
    source = trimmed;
    textSources.set(node, source);
  }
  const translated = translateCore(source);
  if (translated !== source) node.data = withWhitespace(current, translated);
}

function localizeAttributes(element: Element) {
  if (excluded(element)) return;
  let sources = attributeSources.get(element);
  if (!sources) {
    sources = new Map();
    attributeSources.set(element, sources);
  }
  const language = i18n.language === "en" ? "en" : "zh-CN";
  for (const attribute of ATTRIBUTES) {
    const current = element.getAttribute(attribute);
    if (!current) continue;
    let source = sources.get(attribute);
    if (language === "en") {
      if (source && current !== source) element.setAttribute(attribute, source);
      continue;
    }
    if (source && current === translateCore(source)) continue;
    if (!source || (current !== source && !englishToChinese.has(current))) {
      if (!/[A-Za-z]/.test(current) || chineseToEnglish.has(current)) continue;
      source = current;
      sources.set(attribute, source);
    }
    const translated = translateCore(source);
    if (translated !== source) element.setAttribute(attribute, translated);
  }
}

export function localizeSubtree(root: Node = document.body) {
  if (root.nodeType === Node.TEXT_NODE) {
    localizeText(root as Text);
    return;
  }
  if (root.nodeType !== Node.ELEMENT_NODE && root.nodeType !== Node.DOCUMENT_NODE) return;
  if (root.nodeType === Node.ELEMENT_NODE) localizeAttributes(root as Element);
  const walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    if (node.nodeType === Node.TEXT_NODE) localizeText(node as Text);
    else localizeAttributes(node as Element);
  }
}

export function installDomLocalizer() {
  const run = () => localizeSubtree(document.body);
  const observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      if (mutation.type === "characterData") localizeSubtree(mutation.target);
      else if (mutation.type === "attributes") localizeAttributes(mutation.target as Element);
      else mutation.addedNodes.forEach(localizeSubtree);
    }
  });
  const start = () => {
    run();
    observer.observe(document.body, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: [...ATTRIBUTES],
    });
  };
  if (document.body) start();
  else window.addEventListener("DOMContentLoaded", start, { once: true });
  window.addEventListener("condor-language-changed", run);
}
