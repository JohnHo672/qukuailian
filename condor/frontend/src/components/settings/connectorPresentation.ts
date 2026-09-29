export type InterfaceLanguage = "zh-CN" | "en";

const CONNECTOR_LABELS: Record<string, Record<InterfaceLanguage, string>> = {
  okx: { "zh-CN": "OKX 实盘现货", en: "OKX Live Spot" },
  okx_perpetual: { "zh-CN": "OKX 实盘永续", en: "OKX Live Perpetual" },
  okx_demo: { "zh-CN": "OKX 模拟盘现货", en: "OKX Demo Spot" },
  okx_perpetual_demo: { "zh-CN": "OKX 模拟盘永续", en: "OKX Demo Perpetual" },
};

export function connectorDisplayName(name: string, language: InterfaceLanguage): string {
  return CONNECTOR_LABELS[name]?.[language] ?? name;
}

export function isDemoConnector(name: string): boolean {
  return name === "okx_demo" || name === "okx_perpetual_demo";
}

export function connectorDocumentationSlug(name: string): string {
  if (name.startsWith("okx")) return "okx";
  return name.replace(/_(perpetual_demo|demo|perpetual|spot)$/, "");
}

export function friendlyCredentialError(
  message: string,
  connectorName: string,
  language: InterfaceLanguage,
): string {
  const demo = isDemoConnector(connectorName);
  if (message.includes("50101")) {
    return language === "zh-CN"
      ? `${demo ? "模拟盘" : "实盘"}密钥与所选环境不一致。请确认选择了正确的 OKX 实盘/模拟盘入口。（OKX 50101）`
      : `The API key does not match the selected ${demo ? "demo" : "live"} environment. Choose the correct OKX live/demo connector. (OKX 50101)`;
  }
  if (message.includes("50111")) {
    return language === "zh-CN"
      ? "OKX 无法识别这个 API Key。请检查是否复制完整，以及密钥是否仍然有效。（OKX 50111）"
      : "OKX did not recognize this API key. Check that it was copied completely and is still active. (OKX 50111)";
  }
  if (message.includes("50113")) {
    return language === "zh-CN"
      ? "签名校验失败。请重新检查 Secret Key 和 Passphrase，注意不要带空格。（OKX 50113）"
      : "Signature verification failed. Recheck the Secret Key and Passphrase without extra spaces. (OKX 50113)";
  }
  return message;
}

