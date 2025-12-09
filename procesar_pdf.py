#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
procesar_pdf.py
───────────────
Función principal:
    process_pdf(pdf_path: Path, work_dir: Path) -> Path

Devuelve la ruta absoluta del «consolidado.xlsx».
"""

from __future__ import annotations
import re, shutil, uuid
from pathlib import Path
import pdfplumber
import pandas as pd
import numpy as np
from PyPDF2 import PdfReader, PdfWriter


# ────────────────────── UTILIDADES PDF ──────────────────────
def find_retro_index(pdf: Path,
                     header="Informe Retrospectivo de Gestión Individual") -> int:
    hdr = header.lower()
    with pdfplumber.open(str(pdf)) as doc:
        for i, p in enumerate(doc.pages):
            if hdr in (p.extract_text() or "").lower():
                return i
    return -1


def trim_pdf(src: Path, start: int, dst: Path) -> None:
    r, w = PdfReader(str(src)), PdfWriter()
    for i in range(start, len(r.pages)):
        w.add_page(r.pages[i])
    with open(dst, "wb") as f:
        w.write(f)


# ────────────────────── EXTRACCIÓN TABLAS ───────────────────
def extract_tables(pdf: Path) -> int:
    out_dir = pdf.parent / "tablas_extraidas"
    out_dir.mkdir(exist_ok=True)
    n = 0
    with pdfplumber.open(str(pdf)) as doc:
        for page in doc.pages:
            for tb in page.extract_tables() or []:
                n += 1
                pd.DataFrame(tb[1:], columns=tb[0]).to_csv(
                    out_dir / f"{n}.csv", index=False
                )
    return n


def build_consolidated_excel(pdf: Path, n_tables: int) -> Path:
    out_dir   = pdf.parent / "tablas_extraidas"
    xlsx_path = pdf.parent / "salida.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        row0 = 0
        for i in range(1, n_tables + 1):
            csv = out_dir / f"{i}.csv"
            if csv.exists():
                df = pd.read_csv(csv)
                df.to_excel(w, "consolidado", index=False,
                            header=True, startrow=row0)
                row0 += len(df) + 1
    return xlsx_path


# ────────────────────── COLUMNA INTEGRANTE ──────────────────
def add_nombre_integrante_column(xlsx: Path) -> None:
    df = pd.read_excel(xlsx, "consolidado", header=None, engine="openpyxl")
    nombres, actual = [], ""
    ncols = df.shape[1]

    for _, row in df.iterrows():
        for c in range(ncols):
            cell = str(row[c]).strip() if pd.notnull(row[c]) else ""
            if cell == "Nombre del integrante:":
                for off in range(1, 6):
                    nxt = c + off
                    if nxt < ncols:
                        val = str(row[nxt]).strip() if pd.notnull(row[nxt]) else ""
                        if val:
                            actual = val
                            break
                break
        nombres.append(actual)

    df.insert(0, "Nombre integrante", nombres)
    with pd.ExcelWriter(xlsx, engine="openpyxl") as w:
        df.to_excel(w, "consolidado", index=False, header=False)


# ────────────────────── BLOQUES & ANOTACIONES ───────────────
def _extract_blocks(df, pat_start, pat_stops):
    blks, tmp = [], df.copy()
    while True:
        hdr = tmp.iloc[:, 1].astype(str).str.strip()
        hit = hdr.str.match(pat_start)
        if not hit.any():
            break
        s = hit.idxmax()
        blk_start = max(0, s - 2)
        blk_end = len(tmp)
        for st in pat_stops:
            h2 = hdr[s + 1 :].str.match(st)
            if h2.any():
                blk_end = min(blk_end, h2.idxmax())
        blks.append(tmp.iloc[blk_start:blk_end])
        tmp = tmp.drop(tmp.index[blk_start:blk_end]).reset_index(drop=True)
    return blks


def _remove_blocks(df, pat_start, pat_stops):
    tmp = df.copy()
    while True:
        hdr = tmp.iloc[:, 1].astype(str).str.strip()
        hit = hdr.str.match(pat_start)
        if not hit.any():
            break
        s = hit.idxmax()
        blk_start = max(0, s - 2)
        blk_end = len(tmp)
        for st in pat_stops:
            h2 = hdr[s + 1 :].str.match(st)
            if h2.any():
                blk_end = min(blk_end, h2.idxmax())
        tmp = tmp.drop(tmp.index[blk_start:blk_end]).reset_index(drop=True)
    return tmp


def annotate_section(df, suf: str) -> pd.DataFrame:
    cat_pats = [
        re.compile(p, re.I)
        for p in [
            rf"\d+\. Gestión en el plano del hacer - {suf}$",
            rf"\d+\. Gestión en el plano del control\s*-\s*{suf}$",
            rf"\d+\. Desarrollo de habilidades.* - {suf}$",
            rf"\d+\. Gestión en el plano de la comunicación - {suf}$",
        ]
    ]
    sub_pat = re.compile(
        r"^\d+\.\d+\.?\s+(Fuera de la organización|Dentro de la organización)$",
        re.I,
    )

    cat, sub, cats, subs = "", "", [], []
    for _, row in df.iterrows():
        text = str(row.iloc[1]).strip()
        for cp in cat_pats:
            if cp.match(text):
                cat, sub = text, ""
                break
        m = sub_pat.match(text)
        if m:
            sub = m.group(1)
        cats.append(cat)
        subs.append(sub)

    df = df.copy()
    df.insert(1, "Subcategoria", subs)
    df.insert(1, "Categoria", cats)
    return df


# ─────────────────── DIVISIÓN → DATOS.XLSX ──────────────────
def split_salida_to_datos(pdf: Path, xlsx: Path) -> Path:
    df = pd.read_excel(xlsx, "consolidado", header=None, engine="openpyxl")

    m_r = "Informe Retrospectivo de Gestión Individual"
    m_p = "Informe Prospectivo de Gestión Individual"
    retro_rows, pros_rows, sec = [], [], "Retrospectivo"

    for _, row in df.iterrows():
        txt = str(row.iloc[1]).strip()
        if txt == m_p:
            sec = "Prospectivo"
            pros_rows.append(row)
        elif txt == m_r:
            sec = "Retrospectivo"
            retro_rows.append(row)
        else:
            (retro_rows if sec == "Retrospectivo" else pros_rows).append(row)

    retro_df, pros_df = [
        pd.DataFrame(r).reset_index(drop=True) for r in (retro_rows, pros_rows)
    ]

    def pats(sfx):
        start = re.compile(
            rf"^\d+\. Gestión en el plano de la comunicación - {sfx}$", re.I
        )
        stops = [
            re.compile(rf"^\d+\. Desarrollo de habilidades.* - {sfx}$", re.I),
            re.compile(rf"^\d+\. Gestión en el plano del hacer - {sfx}$", re.I),
            re.compile(rf"^\d+\. Gestión en el plano del control.* - {sfx}$", re.I),
        ]
        return start, stops

    com_blocks = []

    st, sp = pats("Retrospectivo")
    com_blocks += _extract_blocks(retro_df, st, sp)
    retro_df = _remove_blocks(retro_df, st, sp)

    st, sp = pats("Prospectivo")
    com_blocks += _extract_blocks(pros_df, st, sp)
    pros_df = _remove_blocks(pros_df, st, sp)

    comunicacion_df = (
        pd.concat(com_blocks, ignore_index=True) if com_blocks else pd.DataFrame()
    )

    if not comunicacion_df.empty:
        tam_r = len(com_blocks[0])          # o  len(comunicacion_df)  (sin tilde)
        origen = ["Retrospectivo"] * tam_r + \
                ["Prospectivo"] * (len(comunicacion_df) - tam_r)
        comunicacion_df.insert(3, "Origen", origen)


    retro_df = annotate_section(retro_df, "Retrospectivo")
    pros_df = annotate_section(pros_df, "Prospectivo")
    comunicacion_df = annotate_section(comunicacion_df, "Retrospectivo")

    # Forzar "Retrospectivo" en toda la columna F (índice 5)
    if not comunicacion_df.empty and comunicacion_df.shape[1] > 5:
        comunicacion_df.iloc[:, 5] = "Retrospectivo"

    datos_xlsx = pdf.parent / "datos.xlsx"
    with pd.ExcelWriter(datos_xlsx, engine="openpyxl") as w:
        retro_df.to_excel(w, "Retrospectivo", index=False, header=False)
        comunicacion_df.to_excel(w, "Comunicación", index=False, header=False)
        pros_df.to_excel(w, "Prospectivo", index=False, header=False)

    return datos_xlsx


# ───────────── LIMPIEZA FINAL → DATOS_CLEAN.XLSX ────────────
def clean_datos_excel(datos_xlsx: Path) -> Path:
    clean_xlsx = datos_xlsx.with_name("datos_clean.xlsx")
    hojas_D = {"retrospectivo", "prospectivo"}
    hoja_com = {"comunicación", "comunicacion"}

    def force_int(s):
        s = pd.to_numeric(s, errors="coerce")
        return s.where(s.notna() & (s % 1 == 0)).astype("Int64")

    def is_int(x):
        if pd.isna(x):
            return False
        if isinstance(x, (int, np.integer)):
            return True
        if isinstance(x, float):
            return x.is_integer()
        if isinstance(x, str):
            return x.strip().isdigit()
        return False

    def drop_empty_cols(df):
        keep = []
        for c in df.columns:
            col = df[c]
            if col.isna().all():
                continue
            if col.dtype == object and col.dropna().str.strip().eq("").all():
                continue
            keep.append(c)
        return df[keep]

    def shift_left_row(row):
        vals = [
            v
            for v in row
            if not (pd.isna(v) or (isinstance(v, str) and v.strip() == ""))
        ]
        return pd.Series(vals + [pd.NA] * (len(row) - len(vals)))

    book = pd.ExcelFile(datos_xlsx)
    with pd.ExcelWriter(clean_xlsx, engine="openpyxl") as w:
        for sh in book.sheet_names:
            df = book.parse(sh)

            if sh.lower() in hojas_D and df.shape[1] > 3:
                df.iloc[:, 3] = force_int(df.iloc[:, 3])
                df = df[df.iloc[:, 3].notna()].drop(df.columns[3], axis=1)

            elif sh.lower() in hoja_com and df.shape[1] > 3:
                mask = df.iloc[:, 3].apply(is_int)
                df = df[mask].drop(df.columns[3], axis=1)

                df = df.apply(shift_left_row, axis=1)
                df = df.dropna(how="all")
                df = drop_empty_cols(df)

            df.to_excel(w, sh, index=False, header=False)
    return clean_xlsx



def fix_hoja_comunicacion(clean_xlsx: Path) -> None:
    # Cargar hoja Comunicación
    book = pd.ExcelFile(clean_xlsx)
    df = pd.read_excel(book, sheet_name="Comunicación", header=None)

    col_c = 2  # índice columna C
    ultimo_valido = None
    nuevas_filas = []

    for _, row in df.iterrows():
        valor_c = str(row[col_c]) if pd.notna(row[col_c]) else ""
        if valor_c.strip().startswith("Dentro") or valor_c.strip().startswith("Fuera"):
            ultimo_valido = valor_c.strip()
            nuevas_filas.append(row.values)
        else:
            nueva = row.values.copy()
            # desplazar celdas a la derecha a partir de col_c
            nueva[col_c+1:len(nueva)] = nueva[col_c:len(nueva)-1]
            nueva[col_c] = ultimo_valido
            nuevas_filas.append(nueva)

    # Crear DataFrame corregido
    df_nuevo = pd.DataFrame(nuevas_filas)

    # Reescribir solo la hoja Comunicación manteniendo las demás
    with pd.ExcelWriter(clean_xlsx, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_nuevo.to_excel(writer, sheet_name="Comunicación", index=False, header=False)


# ───────── CONSOLIDADO FINAL → CONSOLIDADO.XLSX ─────────────
def build_consolidado(clean_xlsx: Path) -> Path:
    header = [
        "Procedimiento",
        "Integrante",
        "Actividad",
        "Tiempo",
        "Donde",
        "Plano",
        "Tipo",
    ]
    out_xlsx = clean_xlsx.with_name("consolidado.xlsx")
    libro = pd.ExcelFile(clean_xlsx)
    frames = []

    def solo_iniciales(texto: str):
        if not isinstance(texto, str):
            return pd.NA
        return "".join(w[0] for w in texto.split() if w and w[0].isupper())

    def recod_plano(texto: str):
        if not isinstance(texto, str):
            return pd.NA
        t = texto.lower()
        if "plano de la comunicación" in t:
            return "comunicación"
        if "plano del control" in t:
            return "control"
        if "plano del hacer" in t:
            return "hacer"
        if "desarrollo de habilidades" in t:
            return "desarrollo de habilidades"
        return texto.strip()

    # Retrospectivo / Prospectivo
    for sh in ["Retrospectivo", "Prospectivo"]:
        if sh not in libro.sheet_names:
            continue
        df = libro.parse(sh, header=None)
        if df.empty:
            continue

        frames.append(
            pd.DataFrame(
                {
                    "Procedimiento": df.iloc[:, 4] if df.shape[1] > 4 else pd.NA,
                    "Integrante": df.iloc[:, 0].apply(solo_iniciales),
                    "Actividad": df.iloc[:, 3] if df.shape[1] > 3 else pd.NA,
                    "Tiempo": df.iloc[:, 5] if df.shape[1] > 5 else pd.NA,
                    "Donde": df.iloc[:, 2] if df.shape[1] > 2 else pd.NA,
                    "Plano": df.iloc[:, 1].apply(recod_plano)
                    if df.shape[1] > 1
                    else pd.NA,
                    "Tipo": sh,
                }
            )
        )

    # Comunicación
    for sh in libro.sheet_names:
        if sh.lower() not in {"comunicación", "comunicacion"}:
            continue
        df = libro.parse(sh, header=None)
        if df.empty:
            continue

        frames.append(
            pd.DataFrame(
                {
                    "Procedimiento": df.iloc[:, 9] if df.shape[1] > 9 else pd.NA,
                    "Integrante": df.iloc[:, 0].apply(solo_iniciales),
                    "Actividad": df.iloc[:, 5] if df.shape[1] > 5 else pd.NA,
                    "Tiempo": df.iloc[:, 10] if df.shape[1] > 10 else pd.NA,
                    "Donde": df.iloc[:, 2] if df.shape[1] > 2 else pd.NA,
                    "Plano": df.iloc[:, 1].apply(recod_plano)
                    if df.shape[1] > 1
                    else pd.NA,
                    "Tipo": df.iloc[:, 4] if df.shape[1] > 4 else pd.NA,
                }
            )
        )

    if not frames:
        raise RuntimeError("No se encontró información para el consolidado.")

    resultado = pd.concat(frames, ignore_index=True)
    resultado["Donde"] = resultado["Donde"].replace(r"^\s*$", pd.NA, regex=True).ffill()

    # NUEVO PASO: recodificar "Dentro/Fuera de la organización" en la columna Donde
    resultado["Donde"] = resultado["Donde"].replace(
        {
            "Dentro de la organización": "DO",
            "Fuera de la organización": "FO",
        }
    )

    # Guardar usando hoja llamada "Consolidado"
    resultado.to_excel(out_xlsx, index=False, header=header, sheet_name="Consolidado")
    return out_xlsx


# ═══════════════════════════════════════════════════════════
# FUNCIÓN PÚBLICA
# ═══════════════════════════════════════════════════════════
def process_pdf(pdf_path: Path | str, work_dir: Path | str) -> Path:
    """
    Ejecuta todo el flujo y devuelve la ruta del 'consolidado.xlsx'.
    work_dir se crea si no existe y **se usa como contenedor aislado**.
    """
    pdf_path = Path(pdf_path)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    # Copiar el PDF dentro del directorio de trabajo con nombre seguro
    pdf_copy = work_dir / "input.pdf"
    shutil.copy(pdf_path, pdf_copy)

    idx = find_retro_index(pdf_copy)
    if idx < 0:
        raise RuntimeError("Sección retrospectiva no encontrada.")

    trimmed = work_dir / "trimmed.pdf"
    trim_pdf(pdf_copy, idx, trimmed)

    n_tables = extract_tables(trimmed)
    salida = build_consolidated_excel(trimmed, n_tables)
    add_nombre_integrante_column(salida)
    datos = split_salida_to_datos(trimmed, salida)
    clean = clean_datos_excel(datos)
    fix_hoja_comunicacion(clean) 
    cons = build_consolidado(clean)
    return cons


# Pequeño test manual
if __name__ == "__main__":
    import sys, uuid, tempfile

    if len(sys.argv) != 2:
        print("Uso: python procesar_pdf.py archivo.pdf")
        sys.exit(1)

    temp_dir = Path(tempfile.gettempdir()) / f"run-{uuid.uuid4().hex}"
    out = process_pdf(sys.argv[1], temp_dir)
    print("Consolidado generado en:", out)

