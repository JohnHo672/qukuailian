"""Install the offline Argos English-to-Chinese model used to build the catalog."""

import json
from pathlib import Path

import argostranslate.package as package


installed = package.get_installed_packages()
model = next(
    (
        item
        for item in installed
        if item.from_code == "en" and item.to_code.startswith("zh")
    ),
    None,
)
if model is None:
    package.update_package_index()
    available = package.get_available_packages()
    model = next(
        item
        for item in available
        if item.from_code == "en" and item.to_code.startswith("zh")
    )
    downloaded_path = model.download()
    package.install_from_path(downloaded_path)

# Argos ships only the compact English EWT tokenizer in this package. Newer
# Stanza resource metadata defaults to the larger "combined" tokenizer and
# would otherwise try to download it during an offline build.
installed_model = next(
    item
    for item in package.get_installed_packages()
    if item.from_code == "en" and item.to_code.startswith("zh")
)
resources_path = Path(installed_model.package_path) / "stanza" / "resources.json"
resources = json.loads(resources_path.read_text(encoding="utf-8"))
resources["en"]["default_processors"] = {"tokenize": "ewt"}
resources["en"].setdefault("packages", {})
resources["en"]["packages"]["default"] = {"tokenize": "ewt"}
resources["en"]["packages"]["ewt"] = {"tokenize": "ewt"}
resources_path.write_text(
    json.dumps(resources, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
print(f"Installed: {model}")
