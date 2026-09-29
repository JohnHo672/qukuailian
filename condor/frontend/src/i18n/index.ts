import i18n from "i18next";
import { initReactI18next } from "react-i18next";

import { LANGUAGE_STORAGE_KEY } from "@/lib/sessionState";
import { generatedEntries } from "./generated";
import { curatedEntries } from "./curated";

export type AppLanguage = "zh-CN" | "en";

const saved = localStorage.getItem(LANGUAGE_STORAGE_KEY);
const initialLanguage: AppLanguage = saved === "en" ? "en" : "zh-CN";

// Generated translations are extraction candidates only. They came from an
// offline machine translator and can be misleading on a trading screen, so an
// unreviewed string deliberately falls back to its English source. Only the
// human-reviewed curated table is allowed to render Chinese.
const reviewedFallbackEntries = generatedEntries.map(({ id, en }) => ({ id, en, zh: en }));
const entries = [...reviewedFallbackEntries, ...curatedEntries];
const en = Object.fromEntries(entries.map(({ id, en }) => [id, en]));
const zh = Object.fromEntries(entries.map(({ id, zh }) => [id, zh]));

void i18n.use(initReactI18next).init({
  resources: {
    en: { translation: { ...en, "language.chinese": "简体中文", "language.english": "English", "language.switch": "Language" } },
    "zh-CN": { translation: { ...zh, "language.chinese": "简体中文", "language.english": "English", "language.switch": "语言" } },
  },
  lng: initialLanguage,
  fallbackLng: "en",
  interpolation: { escapeValue: false },
  returnNull: false,
});

document.documentElement.lang = initialLanguage;

export const englishToChinese = new Map(
  entries.map(({ en, zh }) => [en, zh]),
);
export const curatedEnglishToChinese = new Map(
  curatedEntries.map(({ en, zh }) => [en.toLocaleLowerCase("en"), zh]),
);
export const chineseToEnglish = new Map(
  entries.filter(({ en, zh }) => zh !== en).map(({ en, zh }) => [zh, en]),
);

export async function setAppLanguage(language: AppLanguage) {
  localStorage.setItem(LANGUAGE_STORAGE_KEY, language);
  document.documentElement.lang = language;
  await i18n.changeLanguage(language);
  window.dispatchEvent(new CustomEvent("condor-language-changed", { detail: language }));
}

export default i18n;
