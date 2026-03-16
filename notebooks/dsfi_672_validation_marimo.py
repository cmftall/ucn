import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import os
    from typing import Dict, List, Tuple

    import duckdb
    import marimo as mo

    return Dict, List, Tuple, duckdb, json, mo, os


@app.cell
def _(mo):
    mo.md(
        """
    # Validation Histoire Utilisateur DSFI-672

    Ce notebook valide :
    - un cas **passant sans atypie** (SIRET/IdDsn donné),
    - un cas **non passant avec atypies**,
    - la présence et la cohérence des anomalies **8e5a** et **8e5b**.
    """
    )
    return


@app.cell
def _():
    validation_inputs = {
        "passant": {
            "siret": "05680065900858",
            "iddsn": "WS-iz6XbuVdZreCN9eb3enZ",
        },
        "non_passant": {
            "siret": "05680065901211",
            "iddsn": "WSz3SoH3MiIvLVC1MJyg73C",
        },
        "expected_by_code": {
            "DI_EXO_08e5a_V01": {
                "codeTraitement": "UR_ANO_APP_DIEXO08e5a",
                "libelle": "Incohérence de l'exonération pour les apprentis déclarée en bloc 81 (assiette plafonnée)",
                "type": "CORRECTION_DONNEES_DSN_PRECEDEMENT_DECLAREE",
                "niveau": "ENTREPRISE",
                "controle": "Incohérence données déclarées",
                "actionAttendue": "Correction de l'anomalie",
                "atypie_contains": [
                    "LACLAU Halldora",
                    "CTRAT1",
                    "assiette d'exonération salariale plafonnée",
                    "79 % du SMIC",
                    "900.9000 €",
                ],
                "idAnomalie": "DI_EXO_08e5a_V01_05680065901211_2025",
            },
            "DI_EXO_08e5b_V01": {
                "codeTraitement": "UR_ANO_APP_DIEXO08e5b",
                "libelle": "Incohérence de l'exonération pour les apprentis déclaré en bloc 81 (secteur privé)",
                "type": "CORRECTION_DONNEES_DSN_PRECEDEMENT_DECLAREE",
                "niveau": "ENTREPRISE",
                "controle": "Incohérence données déclarées",
                "actionAttendue": "Correction de l'anomalie",
                "atypie_contains": [
                    "LACLAU Halldora",
                    "CTRAT1",
                    "assiette d'exonération salariale déplafonnée",
                    "79 % du SMIC",
                    "900.9000 €",
                ],
                "idAnomalie": "DI_EXO_08e5b_V01_05680065901211_2025",
            },
        },
    }
    return (validation_inputs,)


@app.cell
def _(os):
    path_peri = "tests/local/test_local_DSFI-672_tfi/outputs/current_captures/DSFI-672/current-dsn_verificationdeclarative_perimetrage_mns.csv.gz"
    path_bilan = "tests/local/test_local_DSFI-672_tfi/outputs/current_captures/DSFI-672/current-dsn_verificationdeclarative_precalculsmonoperiode_pre_bilan.csv.gz"
    files_ok = os.path.exists(path_peri) and os.path.exists(path_bilan)
    return files_ok, path_bilan, path_peri


@app.cell
def _(duckdb, files_ok, path_bilan, path_peri):
    con = None
    if files_ok:
        con = duckdb.connect()
        con.execute(
            f"""
            CREATE OR REPLACE VIEW perimetrage AS
            SELECT
                UrssafNat_Dsn_Collecte_CsvToParquetDeclaratif_SIRET AS SIRET,
                UrssafNat_Dsn_Collecte_DsnToCsv_IdDsn AS IdDsn
            FROM read_csv('{path_peri}', sep=';', header=True, all_varchar=True)
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE VIEW bilan AS
            SELECT
                UrssafNat_Dsn_Collecte_CsvToParquetDeclaratif_SIRET AS SIRET,
                UrssafNat_Dsn_Collecte_DsnToCsv_IdDsn AS IdDsn,
                UrssafNat_Dsn_VerificationDeclarative_Precalculs_Code AS Code,
                UrssafNat_Dsn_VerificationDeclarative_Precalculs_Declenchement AS Declenchement,
                UrssafNat_Dsn_VerificationDeclarative_Precalculs_Data AS Data
            FROM read_csv('{path_bilan}', sep=';', header=True, all_varchar=True)
            """
        )
    return (con,)


@app.cell
def _(con, mo):
    mo.stop(
        con is None,
        mo.md("ERREUR : **Fichiers de captures introuvables. Vérifiez le run TFI.**"),
    )
    return


@app.cell
def _(Dict, List, Tuple, json):
    def _normalize_text(value: str) -> str:
        return " ".join((value or "").split())

    def _safe_parse_json(raw_data: str) -> Dict[str, str]:
        if raw_data is None:
            return {}

        text = str(raw_data).strip()
        if not text:
            return {}

        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {"_raw": text}
        except json.JSONDecodeError:
            return {"_raw": text}

    def _validate_expected_fields(
        payload: Dict[str, str], expected: Dict[str, str]
    ) -> Tuple[bool, List[Dict[str, str]]]:
        checks: List[Dict[str, str]] = []

        for key in [
            "codeTraitement",
            "libelle",
            "type",
            "niveau",
            "controle",
            "actionAttendue",
            "idAnomalie",
        ]:
            actual = str(payload.get(key, ""))
            expected_value = str(expected.get(key, ""))
            ok = _normalize_text(actual) == _normalize_text(expected_value)
            checks.append(
                {
                    "Champ": key,
                    "Attendu": expected_value,
                    "Trouve": actual,
                    "Statut": "OK" if ok else "KO",
                }
            )

        atypie_actual = str(payload.get("atypie", payload.get("_raw", "")))
        for chunk in expected.get("atypie_contains", []):
            ok = _normalize_text(chunk).lower() in _normalize_text(atypie_actual).lower()
            checks.append(
                {
                    "Champ": "atypie_contains",
                    "Attendu": chunk,
                    "Trouve": atypie_actual[:300] + ("..." if len(atypie_actual) > 300 else ""),
                    "Statut": "OK" if ok else "KO",
                }
            )

        return all(line["Statut"] == "OK" for line in checks), checks

    return _safe_parse_json, _validate_expected_fields


@app.cell
def _(con, mo, validation_inputs):
    target_siret = validation_inputs["passant"]["siret"]
    target_iddsn = validation_inputs["passant"]["iddsn"]

    peri_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM perimetrage
        WHERE SIRET = ? AND IdDsn = ?
        """,
        [target_siret, target_iddsn],
    ).fetchone()[0]

    anom_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM bilan
        WHERE SIRET = ? AND IdDsn = ? AND Declenchement = '1'
        """,
        [target_siret, target_iddsn],
    ).fetchone()[0]

    status = "[VALIDE]" if (peri_count > 0 and anom_count == 0) else "[ECHEC]"
    interpretation = (
        "Le SIRET est présent dans le périmétrage et ne déclenche aucune atypie."
        if status == "[VALIDE]"
        else "Le SIRET attendu passant n'est pas trouvé dans l'état attendu (présence sans atypie)."
    )

    mo.vstack(
        [
            mo.md("## Validation 1 : Au moins un SIRET passant sans atypie"),
            mo.md(f"**SIRET** : `{target_siret}`"),
            mo.md(f"**IdDsn** : `{target_iddsn}`"),
            mo.md(f"Périmétrage trouvé : **{peri_count}**"),
            mo.md(f"Atypies déclenchées : **{anom_count}**"),
            mo.md(f"### Résultat : {status}"),
            mo.md(f"**Interprétation** : {interpretation}"),
        ]
    )
    return


@app.cell
def _(con, mo, validation_inputs):
    target_siret = validation_inputs["non_passant"]["siret"]
    target_iddsn = validation_inputs["non_passant"]["iddsn"]

    peri_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM perimetrage
        WHERE SIRET = ? AND IdDsn = ?
        """,
        [target_siret, target_iddsn],
    ).fetchone()[0]

    anomalies_df = con.execute(
        """
        SELECT Code, COUNT(*) AS Occurrence
        FROM bilan
        WHERE SIRET = ? AND IdDsn = ? AND Declenchement = '1'
        GROUP BY Code
        ORDER BY Code
        """,
        [target_siret, target_iddsn],
    ).df()

    observed_codes = set(anomalies_df["Code"].tolist()) if len(anomalies_df) else set()
    required_codes = {"DI_EXO_08e5a_V01", "DI_EXO_08e5b_V01"}
    missing_codes = sorted(required_codes - observed_codes)

    status = "[VALIDE]" if (peri_count > 0 and not missing_codes) else "[ECHEC]"
    interpretation = (
        "Le SIRET non passant est bien en périmètre et déclenche les atypies 8e5a et 8e5b."
        if status == "[VALIDE]"
        else f"Codes manquants pour ce SIRET/IdDsn : {missing_codes if missing_codes else 'Aucun, mais périmètre absent'}."
    )

    mo.vstack(
        [
            mo.md("## Validation 2 : Au moins un SIRET non passant avec atypie"),
            mo.md(f"**SIRET** : `{target_siret}`"),
            mo.md(f"**IdDsn** : `{target_iddsn}`"),
            mo.md(f"Périmétrage trouvé : **{peri_count}**"),
            mo.md("Anomalies observées :"),
            mo.ui.table(anomalies_df),
            mo.md(f"### Résultat : {status}"),
            mo.md(f"**Interprétation** : {interpretation}"),
        ]
    )
    return


@app.cell
def _(
    _safe_parse_json,
    _validate_expected_fields,
    con,
    mo,
    validation_inputs,
):
    target_siret = validation_inputs["non_passant"]["siret"]
    target_iddsn = validation_inputs["non_passant"]["iddsn"]
    expected_by_code = validation_inputs["expected_by_code"]

    anomalies_rows = con.execute(
        """
        SELECT Code, Data
        FROM bilan
        WHERE SIRET = ?
          AND IdDsn = ?
          AND Declenchement = '1'
          AND Code IN ('DI_EXO_08e5a_V01', 'DI_EXO_08e5b_V01')
        """,
        [target_siret, target_iddsn],
    ).fetchall()

    payload_by_code = {}
    for code, raw_data in anomalies_rows:
        payload_by_code[code] = _safe_parse_json(raw_data)

    summary_rows = []
    details_blocks = []
    for code, expected in expected_by_code.items():
        payload = payload_by_code.get(code, {})
        if not payload:
            summary_rows.append(
                {"Code": code, "Statut": "KO", "Détail": "Aucune ligne trouvée dans bilan"}
            )
            details_blocks.append(
                mo.md(f"### {code}\n\nAucune donnée trouvée pour ce code.")
            )
            continue

        passed, check_lines = _validate_expected_fields(payload, expected)
        summary_rows.append(
            {
                "Code": code,
                "Statut": "OK" if passed else "KO",
                "Détail": "Tous les champs attendus correspondent"
                if passed
                else "Écarts détectés dans les champs/texte atypie",
            }
        )
        details_blocks.append(
            mo.vstack(
                [
                    mo.md(f"### {code}"),
                    mo.ui.table(check_lines),
                ]
            )
        )

    global_status = "[VALIDE]" if all(row["Statut"] == "OK" for row in summary_rows) else "[ECHEC]"

    mo.vstack(
        [
            mo.md("## Validation 3 : Contrôle détaillé des contenus JSON d'atypie"),
            mo.ui.table(summary_rows),
            mo.md(f"### Résultat global : {global_status}"),
            *details_blocks,
        ]
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
