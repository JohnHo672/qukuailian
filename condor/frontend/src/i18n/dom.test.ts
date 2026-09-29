/** @vitest-environment jsdom */

import { beforeEach, describe, expect, it } from "vitest";

import i18n from "./index";
import { localizeSubtree, translateTextForDisplay } from "./dom";

describe("Condor localization", () => {
  beforeEach(async () => {
    await i18n.changeLanguage("zh-CN");
  });

  it("keeps safety-critical trading directions unambiguous", () => {
    expect(translateTextForDisplay("Buy")).toBe("买入");
    expect(translateTextForDisplay("Sell")).toBe("卖出");
    expect(translateTextForDisplay("Long")).toBe("做多");
    expect(translateTextForDisplay("Short")).toBe("做空");
    expect(translateTextForDisplay("Reduce Only")).toBe("仅减仓");
    expect(translateTextForDisplay("Stop Loss")).toBe("止损");
    expect(translateTextForDisplay("Take Profit")).toBe("止盈");
    expect(translateTextForDisplay("BUY")).toBe("买入");
    expect(translateTextForDisplay("SELL")).toBe("卖出");
    expect(translateTextForDisplay("LONG")).toBe("做多");
    expect(translateTextForDisplay("SHORT")).toBe("做空");
    expect(translateTextForDisplay("Stop bot")).toBe("停止机器人");
  });

  it("translates dynamic counters and errors", () => {
    expect(translateTextForDisplay("3 bots")).toBe("3 个机器人");
    expect(translateTextForDisplay("Failed to load Portfolio")).toBe("加载资产失败");
    expect(translateTextForDisplay("Share with Condor")).toBe("与 Condor 共享");
    expect(translateTextForDisplay("2h ago")).toBe("2 小时前");
  });

  it("returns the original English text in English mode", async () => {
    await i18n.changeLanguage("en");
    expect(translateTextForDisplay("Create Order")).toBe("Create Order");
  });

  it("removes an English plural suffix after translated trading nouns", () => {
    const button = document.createElement("button");
    button.append("0 ", "Executors", "s");
    document.body.append(button);
    localizeSubtree(button);
    expect(button.textContent).toBe("0 执行器");
    button.remove();
  });
});
