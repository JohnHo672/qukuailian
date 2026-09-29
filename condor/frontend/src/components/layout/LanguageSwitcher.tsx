import { Languages } from "lucide-react";
import { useTranslation } from "react-i18next";

import { setAppLanguage, type AppLanguage } from "@/i18n";

export function LanguageSwitcher() {
  const { i18n, t } = useTranslation();
  const language: AppLanguage = i18n.language === "en" ? "en" : "zh-CN";
  const next: AppLanguage = language === "zh-CN" ? "en" : "zh-CN";

  const switchLanguage = () => {
    void setAppLanguage(next);
    window.location.reload();
  };

  return (
    <button
      type="button"
      onClick={switchLanguage}
      className="flex items-center gap-1.5 rounded px-2 py-1.5 text-xs text-[var(--color-text-muted)] hover:bg-[var(--color-surface-hover)] hover:text-[var(--color-text)]"
      title={t("language.switch")}
      aria-label={t("language.switch")}
    >
      <Languages className="h-3.5 w-3.5" />
      <span>{language === "zh-CN" ? t("language.english") : t("language.chinese")}</span>
    </button>
  );
}
