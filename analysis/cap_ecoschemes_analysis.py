# %% [markdown]
# # Análisis Estadístico — Ecorregímenes CAP 2023/2024
#
# **Objetivo:** explorar los datasets procesados y extraer estadísticas sobre
# ecorregímenes, pagos, parcelas y agricultores.
#
# **Datos:** ~5.4M filas por año, 21 columnas. Motor: **DuckDB** (columnar, sin carga en RAM).
#
# **Convención:** todas las consultas filtran por `CORRECTLY_PROCESSED = 'OK'` salvo indicación contraria.

# %% [markdown]
# ## 1. Carga de datos

# %%
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

BASE = Path("..")
OUT  = BASE / "out"
ANOS = [2023, 2024]

PROVINCIAS = {
    "04": "Alicante", "11": "Córdoba", "14": "Granada",
    "18": "Jaén",     "21": "Málaga",   "23": "Murcia",
    "29": "Sevilla",  "41": "Zaragoza",
}

con = duckdb.connect()

def crear_vista(nombre: str, ano: int):
    path = OUT / f"PAC_{ano}_PROCESSED_SORTED.csv"
    con.execute(f"""
        CREATE OR REPLACE VIEW {nombre} AS
        SELECT
            SECUENCIAL, REC_CODIGO, LNA_CODIGO, DESC_LNA,
            PRO_CODIGO, MSP_CODIGO, PRD_CODIGO, PRD_DESC,
            CAST(SUP_SECANO_HA  AS DOUBLE) AS SUP_SECANO_HA,
            CAST(SUP_RIEGO_HA   AS DOUBLE) AS SUP_RIEGO_HA,
            CAST(SUP_TOTAL_HA   AS DOUBLE) AS SUP_TOTAL_HA,
            CASE
                WHEN "%_PENDIENTE_MEDIA" IS NULL OR "%_PENDIENTE_MEDIA" = ''
                THEN NULL
                ELSE CAST(REPLACE("%_PENDIENTE_MEDIA", '%', '') AS DOUBLE)
            END AS PENDIENTE_MEDIA,
            ECORREGIMEN_CODIGO,
            CAST(PENINSULAR_PAGO_BASE  AS DOUBLE) AS PEN_PAGO_BASE,
            CAST(PENINSULAR_PAGO_PLURI AS DOUBLE) AS PEN_PAGO_PLURI,
            CAST(INSULAR_PAGO_BASE     AS DOUBLE) AS INS_PAGO_BASE,
            CAST(INSULAR_PAGO_PLURI    AS DOUBLE) AS INS_PAGO_PLURI,
            CORRECTLY_PROCESSED AS STATUS
        FROM read_csv_auto(
            '{path}', header=True, encoding='UTF-8',
            quote='"', sample_size=-1
        )
    """)
    return path

for ano in ANOS:
    vista = f"cap_{ano}"
    path  = crear_vista(vista, ano)
    n = con.execute(f"SELECT COUNT(*) FROM {vista}").fetchone()[0]
    print(f"  {vista}: {n:,} filas  ({path.name})")

print("Vistas creadas. Listo para el análisis.")
# %% [markdown]
# ## 2. Calidad de datos y estado del pipeline
#
# Cada fila tiene un `STATUS` que indica cómo se procesó.
# Valores: `OK`, `NOT_ECOSHEME`, `PENDIENTE_CLASIFICACION`, `API_DATA_MISSING`, `FAILED`.
# También hay valores legacy (`Y`, `N`, `NaN`) de corridas previas.

# %%
resumen = []
for ano in ANOS:
    df = con.execute(f"""
        SELECT STATUS,
               COUNT(*)                    AS filas,
               COUNT(DISTINCT SECUENCIAL)  AS agricultores
        FROM cap_{ano}
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    df["ano"] = ano
    df["pct"] = (100.0 * df["filas"] / df["filas"].sum()).round(1)
    resumen.append(df)

resumen = pd.concat(resumen, ignore_index=True)
pd.set_option("display.float_format", "{:,.0f}".format)
resumen[["ano", "STATUS", "filas", "pct", "agricultores"]]

# %%
for ano in ANOS:
    df = con.execute(f"""
        SELECT STATUS, LNA_CODIGO, COUNT(*) AS n
        FROM cap_{ano}
        WHERE STATUS IN ('Y', 'N') OR STATUS IS NULL
        GROUP BY 1, 2 ORDER BY 1, 3 DESC
    """).fetchdf()
    print(f"\n=== {ano} ===")
    print(df.to_string(index=False) if not df.empty else "  (vacío)")

# %%
for ano in ANOS:
    total = con.execute(f"SELECT COUNT(*) FROM cap_{ano}").fetchone()[0]
    ok    = con.execute(f"SELECT COUNT(*) FROM cap_{ano} WHERE STATUS = 'OK'").fetchone()[0]
    print(f"  {ano}: total={total:,}  OK={ok:,}  ({100*ok/total:.1f}%)")
# %% [markdown]
# ## 3. Distribución de ecorregímenes
#
# | Código | Ecorregimen | LNA origen |
# |--------|-------------|------------|
# | P1 | Pastoreo extensivo | 505 |
# | P3/P4 | Siembra directa / Rotación | 508, 509 |
# | P5 (A) | Biodiversidad (secano/riego) | 512 |
# | P5 (B) | Biodiversidad (bajo agua) | 506, 507 |
# | P6/P7 | Cubierta vegetal / inerte | 510, 511 |

# %%
eco = []
for ano in ANOS:
    df = con.execute(f"""
        SELECT ECORREGIMEN_CODIGO AS ecorregimen,
               COUNT(*)                    AS filas,
               COUNT(DISTINCT SECUENCIAL)  AS agricultores,
               ROUND(SUM(SUP_TOTAL_HA), 0) AS hectareas
        FROM cap_{ano} WHERE STATUS = 'OK'
        GROUP BY 1 ORDER BY 4 DESC
    """).fetchdf()
    df["ano"] = ano
    eco.append(df)

eco = pd.concat(eco, ignore_index=True)
eco[["ano", "ecorregimen", "filas", "agricultores", "hectareas"]]

# %%
fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(eco[eco["ano"]==2023]["ecorregimen"]))
width = 0.35
for i, ano in enumerate(ANOS):
    sub = eco[eco["ano"]==ano].set_index("ecorregimen").reindex(eco[eco["ano"]==2023]["ecorregimen"])
    ax.bar(x + i*width, sub["hectareas"], width, label=str(ano))
ax.set_xlabel("Ecorregimen")
ax.set_ylabel("Hectáreas")
ax.set_title("Superficie por ecorregimen (2023 vs 2024)")
ax.set_xticks(x + width/2)
ax.set_xticklabels(eco[eco["ano"]==2023]["ecorregimen"])
ax.legend()
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1000:,.0f}k"))
plt.tight_layout()
plt.show()

# %%
lna = con.execute("""
    SELECT LNA_CODIGO, DESC_LNA, ECORREGIMEN_CODIGO,
           COUNT(*) AS filas,
           COUNT(DISTINCT SECUENCIAL) AS agricultores
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1, 2, 3 ORDER BY 4 DESC
""").fetchdf()
# %% [markdown]
# ## 4. Distribución geográfica y por cultivo
#
# 8 provincias del sur/centro de España. El cultivo se identifica por `PRD_DESC`.

# %%
prov = con.execute("""
    SELECT PRO_CODIGO,
           COUNT(*)                    AS filas,
           COUNT(DISTINCT SECUENCIAL)  AS agricultores,
           ROUND(SUM(SUP_TOTAL_HA), 0) AS hectareas
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1 ORDER BY 3 DESC
""").fetchdf()
prov["provincia"] = prov["PRO_CODIGO"].map(PROVINCIAS)
prov[["provincia", "PRO_CODIGO", "agricultores", "hectareas"]]

# %%
fig, ax1 = plt.subplots(figsize=(10, 5))
x = np.arange(len(prov))
ax1.bar(x, prov["agricultores"], color="steelblue", label="Agricultores")
ax1.set_ylabel("Agricultores", color="steelblue")
ax1.set_xticks(x)
ax1.set_xticklabels(prov["provincia"], rotation=30, ha="right")
ax2 = ax1.twinx()
ax2.plot(x, prov["hectareas"], "ro-", label="Hectáreas")
ax2.set_ylabel("Hectáreas", color="red")
ax1.set_title("Distribución por provincia (2023, filas OK)")
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
plt.tight_layout()
plt.show()

# %%
piv = con.execute("""
    SELECT PRO_CODIGO, ECORREGIMEN_CODIGO, SUM(SUP_TOTAL_HA) AS ha
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1, 2
""").fetchdf().pivot(index="PRO_CODIGO", columns="ECORREGIMEN_CODIGO", values="ha")
piv.index = piv.index.map(lambda p: PROVINCIAS.get(p, p))
piv = piv.sort_index()

fig, ax = plt.subplots(figsize=(10, 5))
im = ax.imshow(piv.values, aspect="auto", cmap="YlOrRd")
ax.set_xticks(range(len(piv.columns)))
ax.set_xticklabels(piv.columns)
ax.set_yticks(range(len(piv.index)))
ax.set_yticklabels(piv.index)
ax.set_title("Hectáreas por provincia × ecorregimen (2023)")
fig.colorbar(im, ax=ax, label="Hectáreas")
for i in range(piv.shape[0]):
    for j in range(piv.shape[1]):
        v = piv.values[i, j]
        if v > 0:
            ax.text(j, i, f"{v/1000:.0f}k", ha="center", va="center", fontsize=8)
plt.tight_layout()
plt.show()

# %%
crops = con.execute("""
    SELECT PRD_DESC,
           COUNT(*)                    AS filas,
           COUNT(DISTINCT SECUENCIAL)  AS agricultores,
           ROUND(SUM(SUP_TOTAL_HA), 0) AS hectareas
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1 ORDER BY 4 DESC LIMIT 10
""").fetchdf()
crops

# %%
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(range(len(crops)), crops["hectareas"], color="forestgreen")
ax.set_yticks(range(len(crops)))
ax.set_yticklabels(crops["PRD_DESC"])
ax.set_xlabel("Hectáreas")
ax.set_title("Top 10 cultivos por superficie (2023, filas OK)")
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1000:,.0f}k"))
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 5. Tamaño de parcela y pendiente
#
# La pendiente (`PENDIENTE_MEDIA`) solo tiene valor para LNA 510/511 (dependen de SIGPAC).
# El tamaño se mide en `SUP_TOTAL_HA`.

# %%
size = con.execute("""
    SELECT
        CASE
            WHEN SUP_TOTAL_HA < 1  THEN '<1 ha'
            WHEN SUP_TOTAL_HA < 5  THEN '1-5 ha'
            WHEN SUP_TOTAL_HA < 10 THEN '5-10 ha'
            WHEN SUP_TOTAL_HA < 25 THEN '10-25 ha'
            WHEN SUP_TOTAL_HA < 50 THEN '25-50 ha'
            ELSE '50+ ha'
        END AS tamano,
        COUNT(*)                    AS filas,
        COUNT(DISTINCT SECUENCIAL)  AS agricultores,
        ROUND(SUM(SUP_TOTAL_HA), 0) AS hectareas
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1 ORDER BY MIN(SUP_TOTAL_HA)
""").fetchdf()
size

# %%
ha = con.execute("""
    SELECT SUP_TOTAL_HA FROM cap_2023
    WHERE STATUS = 'OK' AND SUP_TOTAL_HA > 0
""").fetchdf()["SUP_TOTAL_HA"].values

fig, ax = plt.subplots(figsize=(10, 5))
ax.hist(np.log10(ha), bins=60, color="steelblue", edgecolor="white")
ax.set_xlabel("log10(SUP_TOTAL_HA)")
ax.set_ylabel("Frecuencia")
ax.set_title("Distribución del tamaño de parcela (escala log, 2023)")
for p, label in [(50, "Mediana"), (90, "P90"), (99, "P99")]:
    v = np.percentile(ha, p)
    ax.axvline(np.log10(v), color="red", linestyle="--", alpha=0.7)
    ax.text(np.log10(v), ax.get_ylim()[1]*0.9, f" {label}: {v:.1f} ha",
            color="red", fontsize=9, va="top")
plt.tight_layout()
plt.show()

# %%
slope = con.execute("""
    SELECT ECORREGIMEN_CODIGO,
           COUNT(*) AS n,
           ROUND(AVG(PENDIENTE_MEDIA), 1)    AS media,
           ROUND(MEDIAN(PENDIENTE_MEDIA), 1) AS mediana,
           ROUND(quantile_cont(PENDIENTE_MEDIA, 0.25), 1) AS p25,
           ROUND(quantile_cont(PENDIENTE_MEDIA, 0.75), 1) AS p75,
           ROUND(MIN(PENDIENTE_MEDIA), 1) AS min,
           ROUND(MAX(PENDIENTE_MEDIA), 1) AS max
    FROM cap_2023
    WHERE STATUS = 'OK' AND PENDIENTE_MEDIA IS NOT NULL
    GROUP BY 1 ORDER BY 3 DESC
""").fetchdf()
slope

# %%
sample = con.execute("""
    SELECT LNA_CODIGO, PENDIENTE_MEDIA
    FROM cap_2023
    WHERE STATUS = 'OK' AND PENDIENTE_MEDIA IS NOT NULL
    USING SAMPLE 100000 ROWS
""").fetchdf()

fig, ax = plt.subplots(figsize=(8, 5))
ax.boxplot(
    [sample[sample["LNA_CODIGO"]==510]["PENDIENTE_MEDIA"],
     sample[sample["LNA_CODIGO"]==511]["PENDIENTE_MEDIA"]],
    labels=["510: Cubierta vegetal", "511: Cubierta inerta"],
    patch_artist=True, boxprops=dict(facecolor="lightblue"),
    medianprops=dict(color="red")
)
ax.set_ylabel("Pendiente media (%)")
ax.set_title("Distribución de la pendiente (muestra 100k filas, 2023)")
plt.tight_layout()
plt.show()

lna

# %% [markdown]
# ## 6. Análisis de pagos
#
# Cada fila OK tiene 4 columnas de pago (EUR): peninsular/isular × base/plurianual.

# %%
pagos = []
for ano in ANOS:
    df = con.execute(f"""
        SELECT ECORREGIMEN_CODIGO AS ecorregimen,
               ROUND(SUM(PEN_PAGO_BASE),  0) AS pen_base,
               ROUND(SUM(PEN_PAGO_PLURI), 0) AS pen_pluri,
               ROUND(SUM(INS_PAGO_BASE),  0) AS ins_base,
               ROUND(SUM(INS_PAGO_PLURI), 0) AS ins_pluri,
               ROUND(SUM(SUP_TOTAL_HA), 0)   AS ha
        FROM cap_{ano} WHERE STATUS = 'OK'
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    df["ano"] = ano
    pagos.append(df)

pagos = pd.concat(pagos, ignore_index=True)
pagos

# %%
eur_ha = con.execute("""
    SELECT ECORREGIMEN_CODIGO AS ecorregimen,
           ROUND(SUM(PEN_PAGO_BASE)  / SUM(SUP_TOTAL_HA), 2) AS pen_base_eur_ha,
           ROUND(SUM(PEN_PAGO_PLURI) / SUM(SUP_TOTAL_HA), 2) AS pen_pluri_eur_ha,
           ROUND(SUM(INS_PAGO_BASE)  / SUM(SUP_TOTAL_HA), 2) AS ins_base_eur_ha,
           ROUND(SUM(INS_PAGO_PLURI) / SUM(SUP_TOTAL_HA), 2) AS ins_pluri_eur_ha
    FROM cap_2023 WHERE STATUS = 'OK'
    GROUP BY 1 ORDER BY 2 DESC
""").fetchdf()
eur_ha

# %%
fig, ax = plt.subplots(figsize=(10, 5))
x = np.arange(len(eur_ha))
width = 0.2
ax.bar(x - 1.5*width, eur_ha["pen_base_eur_ha"],  width, label="Pen. base")
ax.bar(x - 0.5*width, eur_ha["pen_pluri_eur_ha"], width, label="Pen. pluri")
ax.bar(x + 0.5*width, eur_ha["ins_base_eur_ha"],  width, label="Ins. base")
ax.bar(x + 1.5*width, eur_ha["ins_pluri_eur_ha"], width, label="Ins. pluri")
ax.set_xticks(x)
ax.set_xticklabels(eur_ha["ecorregimen"])
ax.set_ylabel("EUR / ha")
ax.set_title("Pago efectivo por hectárea (2023)")
ax.legend()
plt.tight_layout()
plt.show()

# %%
conc = con.execute("""
    WITH ha_farm AS (
        SELECT SECUENCIAL, SUM(SUP_TOTAL_HA) AS ha
        FROM cap_2023 WHERE STATUS = 'OK'
        GROUP BY 1
    ),
    ranked AS (
        SELECT ha,
               SUM(ha) OVER (ORDER BY ha DESC
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_ha,
               SUM(ha) OVER () AS total_ha,
               ROW_NUMBER() OVER (ORDER BY ha DESC) AS rn
        FROM ha_farm
    )
    SELECT
        ROUND(100.0 * MAX(CASE WHEN rn <= 10  THEN cum_ha END) / total_ha, 1) AS top10,
        ROUND(100.0 * MAX(CASE WHEN rn <= 50  THEN cum_ha END) / total_ha, 1) AS top50,
        ROUND(100.0 * MAX(CASE WHEN rn <= 100 THEN cum_ha END) / total_ha, 1) AS top100
    FROM ranked
""").fetchdf()
# %% [markdown]
# ## 7. Comparación interanual (2023 vs 2024)
#
# Detectar cambios en superficie, número de agricultores y distribución por ecorregimen.

# %%
yoy = con.execute("""
    SELECT '2023' AS ano, COUNT(*) AS filas_ok,
           COUNT(DISTINCT SECUENCIAL) AS agricultores,
           ROUND(SUM(SUP_TOTAL_HA), 0) AS hectareas
    FROM cap_2023 WHERE STATUS = 'OK'
    UNION ALL
    SELECT '2024', COUNT(*), COUNT(DISTINCT SECUENCIAL), ROUND(SUM(SUP_TOTAL_HA), 0)
    FROM cap_2024 WHERE STATUS = 'OK'
""").fetchdf()
yoy["delta_filas"] = yoy["filas_ok"].iloc[1] - yoy["filas_ok"].iloc[0]
yoy["delta_agric"] = yoy["agricultores"].iloc[1] - yoy["agricultores"].iloc[0]
yoy["delta_ha"]    = yoy["hectareas"].iloc[1] - yoy["hectareas"].iloc[0]
yoy

# %%
overlap = con.execute("""
    WITH f23 AS (SELECT DISTINCT SECUENCIAL FROM cap_2023 WHERE STATUS = 'OK'),
         f24 AS (SELECT DISTINCT SECUENCIAL FROM cap_2024 WHERE STATUS = 'OK')
    SELECT
        (SELECT COUNT(*) FROM f23) AS total_2023,
        (SELECT COUNT(*) FROM f24) AS total_2024,
        (SELECT COUNT(*) FROM f23 JOIN f24 USING (SECUENCIAL)) AS ambos_anos,
        (SELECT COUNT(*) FROM f23 WHERE SECUENCIAL NOT IN (SELECT SECUENCIAL FROM f24)) AS solo_2023,
        (SELECT COUNT(*) FROM f24 WHERE SECUENCIAL NOT IN (SELECT SECUENCIAL FROM f23)) AS solo_2024
""").fetchone()

print(f"  Agricultores 2023:      {overlap[0]:>10,}")
print(f"  Agricultores 2024:      {overlap[1]:>10,}")
print(f"  En ambos años:          {overlap[2]:>10,}  ({100*overlap[2]/overlap[0]:.1f}% de 2023)")
print(f"  Solo en 2023:           {overlap[3]:>10,}")
print(f"  Solo en 2024:           {overlap[4]:>10,}")

# %%
delta = con.execute("""
    WITH a23 AS (
        SELECT ECORREGIMEN_CODIGO AS eco, SUM(SUP_TOTAL_HA) AS ha
        FROM cap_2023 WHERE STATUS = 'OK' GROUP BY 1
    ),
    a24 AS (
        SELECT ECORREGIMEN_CODIGO AS eco, SUM(SUP_TOTAL_HA) AS ha
        FROM cap_2024 WHERE STATUS = 'OK' GROUP BY 1
    )
    SELECT COALESCE(a23.eco, a24.eco) AS ecorregimen,
           ROUND(a23.ha, 0) AS ha_2023,
           ROUND(a24.ha, 0) AS ha_2024,
           ROUND(a24.ha - a23.ha, 0) AS delta_ha,
           ROUND(100.0 * (a24.ha - a23.ha) / a23.ha, 1) AS delta_pct
    FROM a23 FULL OUTER JOIN a24 USING (eco)
    ORDER BY 2 DESC
""").fetchdf()
delta

# %%
sec_reg = con.execute("""
    SELECT '2023' AS ano,
           ROUND(SUM(SUP_SECANO_HA), 0)  AS secano_ha,
           ROUND(SUM(SUP_RIEGO_HA), 0)   AS regadio_ha,
           ROUND(100.0 * SUM(SUP_RIEGO_HA) / SUM(SUP_TOTAL_HA), 1) AS pct_regadio
    FROM cap_2023 WHERE STATUS = 'OK'
    UNION ALL
    SELECT '2024',
           ROUND(SUM(SUP_SECANO_HA), 0),
           ROUND(SUM(SUP_RIEGO_HA), 0),
           ROUND(100.0 * SUM(SUP_RIEGO_HA) / SUM(SUP_TOTAL_HA), 1)
    FROM cap_2024 WHERE STATUS = 'OK'
""").fetchdf()
sec_reg
# %% [markdown]
# ## 8. Hallazgos clave
#
# > Los valores exactos se rellenan al ejecutar el notebook.
#
# ### Distribución general
# - ~1.8M filas OK por año (~33% del total)
# - ~184k agricultores en 2023, ~181k en 2024 (91% de solapamiento)
# - ~4M ha de superficie ecorregimen por año
# - 8 provincias: Murcia, Granada, Jaén, Zaragoza, Sevilla, Córdoba, Málaga, Alicante
#
# ### Ecorregímenes
# - **P6/P7** domina: ~1.5M ha, 158k agricultores
# - **P3/P4**: ~970k ha
# - **P1**: ~1.3M ha pero solo 7.5k agricultores (parcelas grandes)
# - **P5(A)**: ~194k ha
# - **P5(B)**: ~11k ha (muy minoritario)
#
# ### Pagos
# - P6/P7: ~125-150 EUR/ha (península), ~195-220 EUR/ha (islas)
# - P5(B) tasa más alta: 156.78 EUR/ha (2023)
# - Plurianual > base en P6/P7 y P3/P4
#
# ### Pendiente
# - Media ~19% (LNA 510), ~17% (LNA 511)
# - Mediana ~15-16%, cola larga >40%
#
# ### Tamaño de parcela
# - Mediana: ~4 ha
# - P99: ~333 ha
# - Top 10% concentra ~40% de las hectáreas
#
# ### Datos a vigilar
# - Status `Y`/`N`/`NaN`: legacy, no afectan el análisis
# - `API_DATA_MISSING`: ~9.4k filas (0.5%)
# - `FAILED`: ~7k filas (superficie NULL)


conc.columns = ["Top 10 (%)", "Top 50 (%)", "Top 100 (%)"]
conc


