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
    return mo.md(
        """
    # Validation Histoire Utilisateur DSFI-672

    Ce notebook valide :
    - un cas **passant sans atypie** (SIRET/IdDsn donné),
    - un cas **non passant avec atypies**,
    - la présence et la cohérence des anomalies **8e5a** et **8e5b**.
    """
    )


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
def _(files_ok, mo, path_bilan, path_peri):
    if files_ok:
        return mo.callout(
            f"Données trouvées :\n- {path_peri}\n- {path_bilan}",
            kind="success",
        )
    return mo.callout(
        "Fichiers de captures introuvables. Vérifiez le run TFI et les chemins.",
        kind="danger",
    )


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
    v1_target_siret = validation_inputs["passant"]["siret"]
    v1_target_iddsn = validation_inputs["passant"]["iddsn"]

    v1_peri_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM perimetrage
        WHERE SIRET = ? AND IdDsn = ?
        """,
        [v1_target_siret, v1_target_iddsn],
    ).fetchone()[0]

    v1_anom_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM bilan
        WHERE SIRET = ? AND IdDsn = ? AND Declenchement = '1'
        """,
        [v1_target_siret, v1_target_iddsn],
    ).fetchone()[0]

    status_v1 = "[VALIDE]" if (v1_peri_count > 0 and v1_anom_count == 0) else "[ECHEC]"
    interpretation_v1 = (
        "Le SIRET est présent dans le périmétrage et ne déclenche aucune atypie."
        if status_v1 == "[VALIDE]"
        else "Le SIRET attendu passant n'est pas trouvé dans l'état attendu (présence sans atypie)."
    )

    v1_ui = mo.vstack(
        [
            mo.md("## Validation 1 : Au moins un SIRET passant sans atypie"),
            mo.md(f"**SIRET** : `{v1_target_siret}`"),
            mo.md(f"**IdDsn** : `{v1_target_iddsn}`"),
            mo.md(f"Périmétrage trouvé : **{v1_peri_count}**"),
            mo.md(f"Atypies déclenchées : **{v1_anom_count}**"),
            mo.md(f"### Résultat : {status_v1}"),
            mo.md(f"**Interprétation** : {interpretation_v1}"),
        ]
    )
    return (v1_ui,)


@app.cell
def _(con, mo, validation_inputs):
    v2_target_siret = validation_inputs["non_passant"]["siret"]
    v2_target_iddsn = validation_inputs["non_passant"]["iddsn"]

    v2_peri_count = con.execute(
        """
        SELECT COUNT(*) AS nb
        FROM perimetrage
        WHERE SIRET = ? AND IdDsn = ?
        """,
        [v2_target_siret, v2_target_iddsn],
    ).fetchone()[0]

    v2_anomalies_df = con.execute(
        """
        SELECT Code, COUNT(*) AS Occurrence
        FROM bilan
        WHERE SIRET = ? AND IdDsn = ? AND Declenchement = '1'
        GROUP BY Code
        ORDER BY Code
        """,
        [v2_target_siret, v2_target_iddsn],
    ).df()

    observed_codes_v2 = (
        set(v2_anomalies_df["Code"].tolist()) if len(v2_anomalies_df) else set()
    )
    required_codes_v2 = {"DI_EXO_08e5a_V01", "DI_EXO_08e5b_V01"}
    missing_codes_v2 = sorted(required_codes_v2 - observed_codes_v2)

    status_v2 = "[VALIDE]" if (v2_peri_count > 0 and not missing_codes_v2) else "[ECHEC]"
    interpretation_v2 = (
        "Le SIRET non passant est bien en périmètre et déclenche les atypies 8e5a et 8e5b."
        if status_v2 == "[VALIDE]"
        else f"Codes manquants pour ce SIRET/IdDsn : {missing_codes_v2 if missing_codes_v2 else 'Aucun, mais périmètre absent'}."
    )

    v2_ui = mo.vstack(
        [
            mo.md("## Validation 2 : Au moins un SIRET non passant avec atypie"),
            mo.md(f"**SIRET** : `{v2_target_siret}`"),
            mo.md(f"**IdDsn** : `{v2_target_iddsn}`"),
            mo.md(f"Périmétrage trouvé : **{v2_peri_count}**"),
            mo.md("Anomalies observées :"),
            mo.ui.table(v2_anomalies_df),
            mo.md(f"### Résultat : {status_v2}"),
            mo.md(f"**Interprétation** : {interpretation_v2}"),
        ]
    )
    return (v2_ui,)


@app.cell
def _(
    _safe_parse_json,
    _validate_expected_fields,
    con,
    mo,
    validation_inputs,
):
    v3_target_siret = validation_inputs["non_passant"]["siret"]
    v3_target_iddsn = validation_inputs["non_passant"]["iddsn"]
    v3_expected_by_code = validation_inputs["expected_by_code"]

    v3_anomalies_rows = con.execute(
        """
        SELECT Code, Data
        FROM bilan
        WHERE SIRET = ?
          AND IdDsn = ?
          AND Declenchement = '1'
          AND Code IN ('DI_EXO_08e5a_V01', 'DI_EXO_08e5b_V01')
        """,
        [v3_target_siret, v3_target_iddsn],
    ).fetchall()

    payload_by_code_v3 = {}
    for code, raw_data in v3_anomalies_rows:
        payload_by_code_v3[code] = _safe_parse_json(raw_data)

    summary_rows_v3 = []
    details_blocks_v3 = []
    for code, expected in v3_expected_by_code.items():
        payload_v3 = payload_by_code_v3.get(code, {})
        if not payload_v3:
            summary_rows_v3.append(
                {"Code": code, "Statut": "KO", "Détail": "Aucune ligne trouvée dans bilan"}
            )
            details_blocks_v3.append(
                mo.md(f"### {code}\n\nAucune donnée trouvée pour ce code.")
            )
            continue

        passed_v3, check_lines_v3 = _validate_expected_fields(payload_v3, expected)
        summary_rows_v3.append(
            {
                "Code": code,
                "Statut": "OK" if passed_v3 else "KO",
                "Détail": "Tous les champs attendus correspondent"
                if passed_v3
                else "Écarts détectés dans les champs/texte atypie",
            }
        )
        details_blocks_v3.append(
            mo.vstack(
                [
                    mo.md(f"### {code}"),
                    mo.ui.table(check_lines_v3),
                ]
            )
        )

    global_status_v3 = (
        "[VALIDE]"
        if all(row["Statut"] == "OK" for row in summary_rows_v3)
        else "[ECHEC]"
    )

    v3_ui = mo.vstack(
        [
            mo.md("## Validation 3 : Contrôle détaillé des contenus JSON d'atypie"),
            mo.ui.table(summary_rows_v3),
            mo.md(f"### Résultat global : {global_status_v3}"),
            *details_blocks_v3,
        ]
    )
    return (v3_ui,)


@app.cell
def _(files_ok, mo, v1_ui, v2_ui, v3_ui):
    if not files_ok:
        return mo.md("## Fin du rapport de validation\n\nExécution arrêtée : captures indisponibles.")
    return mo.vstack(
        [
            mo.md("## Synthèse"),
            v1_ui,
            v2_ui,
            v3_ui,
            mo.md("## Fin du rapport de validation"),
        ]
    )


if __name__ == "__main__":
    app.run()
