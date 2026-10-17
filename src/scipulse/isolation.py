"""Exécution d'une fonction dans un processus enfant (traitement par paquets).

La mémoire utilisée par l'enfant est entièrement libérée quand il se termine :
pandas, Great Expectations et le client Elasticsearch ne rendent pas toute leur
mémoire d'un paquet à l'autre.

os.fork est utilisé directement : Airflow lance les tâches dans un processus
« démon », où le module multiprocessing interdit de créer des processus enfants.
"""
from __future__ import annotations

import os
import pickle
import sys


def run_isolated(func, *args):
    """Exécute func(*args) dans un processus enfant et renvoie son résultat."""
    sys.stdout.flush()                             # évite de dupliquer les affichages dans l'enfant
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:                                   # processus enfant
        os.close(read_fd)
        try:
            payload = ("ok", func(*args))
        except Exception as exc:
            payload = ("erreur", repr(exc))
        with os.fdopen(write_fd, "wb") as pipe:
            pickle.dump(payload, pipe)
        os._exit(0)

    os.close(write_fd)                             # processus parent
    with os.fdopen(read_fd, "rb") as pipe:
        status, result = pickle.load(pipe)
    os.waitpid(pid, 0)
    if status != "ok":
        raise RuntimeError(result)
    return result
