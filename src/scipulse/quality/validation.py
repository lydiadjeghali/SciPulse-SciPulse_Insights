"""Exécution des suites Great Expectations et génération des Data Docs."""
from __future__ import annotations

import pandas as pd

from scipulse import config, monitoring
from scipulse.quality.suites import SUITES

DATASOURCE = "scipulse_silver"


def get_context():
    """Projet Great Expectations du dépôt (dossier gx/), créé au besoin."""
    import great_expectations as gx

    if not (config.GE_ROOT / "great_expectations.yml").exists():
        return gx.data_context.FileDataContext.create(project_root_dir=str(config.HOME))
    return gx.get_context(context_root_dir=str(config.GE_ROOT))


def save_suites(context=None) -> list[str]:
    """Enregistre les suites définies dans suites.py (fichiers JSON du dossier expectations/)."""
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.expectation_configuration import ExpectationConfiguration

    context = context or get_context()
    for name, rules in SUITES.items():
        suite = ExpectationSuite(
            expectation_suite_name=name,
            expectations=[
                ExpectationConfiguration(expectation_type=kind, kwargs=kwargs, meta={"notes": note})
                for kind, kwargs, note in rules
            ],
        )
        context.add_or_update_expectation_suite(expectation_suite=suite)
    return list(SUITES)


def validate(df: pd.DataFrame, suite_name: str, run_name: str) -> dict:
    """Valide un DataFrame avec une suite ; met à jour les Data Docs ; renvoie un résumé."""
    context = get_context()
    save_suites(context)

    datasource = context.sources.add_or_update_pandas(DATASOURCE)
    asset = (datasource.get_asset(suite_name) if suite_name in datasource.get_asset_names()
             else datasource.add_dataframe_asset(suite_name))
    checkpoint = context.add_or_update_checkpoint(
        name=f"{suite_name}_checkpoint",
        validations=[{"batch_request": asset.build_batch_request(dataframe=df),
                      "expectation_suite_name": suite_name}],
    )
    result = checkpoint.run(run_name=run_name)

    validation = list(result.run_results.values())[0]["validation_result"]
    stats = validation.statistics
    failed = [
        f"{r.expectation_config.expectation_type}({r.expectation_config.kwargs.get('column', '')})"
        for r in validation.results if not r.success
    ]
    summary = {
        "suite": suite_name,
        "success": bool(validation.success),
        "evaluated": stats["evaluated_expectations"],
        "successful": stats["successful_expectations"],
        "success_percent": round(stats["success_percent"] or 0, 2),
        "failed": failed,
    }
    source = suite_name.split("_")[0]
    monitoring.emit("validation", "silver", source, status="success" if summary["success"] else "failed",
                    records=len(df), suite=suite_name, run_name=run_name,
                    expectations=summary["evaluated"], failed_expectations=len(failed),
                    success_percent=summary["success_percent"])
    return summary
