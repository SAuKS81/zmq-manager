import os
import shutil

target_dir = "collected_go_files"
exclude_dir = "save"

os.makedirs(target_dir, exist_ok=True)

for root, dirs, files in os.walk("."):
    # 'save' und Zielordner vom Durchsuchen ausschließen
    dirs[:] = [d for d in dirs if d not in {exclude_dir, target_dir}]

    for file in files:
        if file.endswith(".go"):
            src_path = os.path.join(root, file)
            # relativen Pfad ohne führenden "./" bereinigen
            rel_path = os.path.relpath(src_path, ".")
            # Pfadtrenner durch Unterstriche ersetzen, um eindeutige Dateinamen zu erzeugen
            safe_name = rel_path.replace(os.sep, "_") + ".txt"
            dst_path = os.path.join(target_dir, safe_name)
            shutil.copy2(src_path, dst_path)
            print(f"Kopiert: {rel_path} -> {safe_name}")

print("Fertig!")
