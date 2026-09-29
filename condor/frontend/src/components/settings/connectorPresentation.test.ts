import { describe, expect, it } from "vitest";

import {
  connectorDisplayName,
  connectorDocumentationSlug,
  friendlyCredentialError,
  isDemoConnector,
} from "./connectorPresentation";

describe("OKX connector presentation", () => {
  it("distinguishes live and demo connectors in both languages", () => {
    expect(connectorDisplayName("okx_demo", "zh-CN")).toBe("OKX 模拟盘现货");
    expect(connectorDisplayName("okx_perpetual_demo", "en")).toBe("OKX Demo Perpetual");
    expect(connectorDisplayName("binance", "zh-CN")).toBe("binance");
    expect(isDemoConnector("okx_demo")).toBe(true);
    expect(isDemoConnector("okx")).toBe(false);
  });

  it("uses the base OKX documentation page for every OKX environment", () => {
    expect(connectorDocumentationSlug("okx_perpetual_demo")).toBe("okx");
    expect(connectorDocumentationSlug("okx_demo")).toBe("okx");
  });

  it("turns environment and authentication codes into actionable Chinese", () => {
    expect(friendlyCredentialError("request failed 50101", "okx_demo", "zh-CN")).toContain(
      "模拟盘密钥与所选环境不一致",
    );
    expect(friendlyCredentialError("request failed 50113", "okx_demo", "zh-CN")).toContain(
      "Secret Key 和 Passphrase",
    );
  });
});

