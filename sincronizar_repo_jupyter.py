from pathlib import Path
import shutil
import filecmp
from datetime import datetime
import sys


# ============================================================
# CONFIGURACIÓN - CAMBIA SOLO ESTAS DOS RUTAS
# ============================================================

CARPETA_COMPI = Path(
    r"\\servidor\carpeta\carpeta_compi"
)

MI_REPO = Path(
    r"C:\ruta\de\mi\repositorio"
)


# Carpetas que nunca queremos copiar
IGNORAR = {
    ".git",
    "venv",
    ".venv",
    "__pycache__",
    ".ipynb_checkpoints"
}


# ============================================================
# FUNCIONES
# ============================================================

def debe_ignorarse(archivo: Path, raiz: Path) -> bool:
    """Comprueba si el archivo está dentro de una carpeta ignorada."""
    ruta_relativa = archivo.relative_to(raiz)

    return any(
        parte in IGNORAR
        for parte in ruta_relativa.parts
    )


def comparar():
    """
    Compara la carpeta del compañero con mi repositorio.

    NO modifica nada.
    """

    nuevos = []
    modificados = []
    iguales = []

    for origen in CARPETA_COMPI.rglob("*"):

        if not origen.is_file():
            continue

        if debe_ignorarse(origen, CARPETA_COMPI):
            continue

        relativa = origen.relative_to(CARPETA_COMPI)
        destino = MI_REPO / relativa

        if not destino.exists():

            nuevos.append((origen, destino, relativa))

        elif not filecmp.cmp(origen, destino, shallow=False):

            modificados.append((origen, destino, relativa))

        else:

            iguales.append(relativa)

    return nuevos, modificados, iguales


def mostrar_resultado(nuevos, modificados, iguales):

    print("\n" + "=" * 65)
    print("COMPARACIÓN CARPETA COMPI -> MI REPOSITORIO")
    print("=" * 65)

    if nuevos:

        print("\nARCHIVOS NUEVOS:")

        for _, _, relativa in nuevos:
            print(f"  [NUEVO]       {relativa}")

    if modificados:

        print("\nARCHIVOS MODIFICADOS:")

        for _, _, relativa in modificados:
            print(f"  [MODIFICADO]  {relativa}")

    print("\n" + "-" * 65)

    print(f"Nuevos:       {len(nuevos)}")
    print(f"Modificados:  {len(modificados)}")
    print(f"Iguales:      {len(iguales)}")

    print("-" * 65)


def sincronizar(nuevos, modificados):

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    backup = MI_REPO.parent / f"backup_sync_{timestamp}"

    # --------------------------------------------------------
    # ARCHIVOS MODIFICADOS
    # --------------------------------------------------------

    for origen, destino, relativa in modificados:

        # Primero backup
        backup_archivo = backup / relativa

        backup_archivo.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        shutil.copy2(
            destino,
            backup_archivo
        )

        # Después sustituimos
        shutil.copy2(
            origen,
            destino
        )

        print(f"[ACTUALIZADO] {relativa}")

    # --------------------------------------------------------
    # ARCHIVOS NUEVOS
    # --------------------------------------------------------

    for origen, destino, relativa in nuevos:

        destino.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        shutil.copy2(
            origen,
            destino
        )

        print(f"[COPIADO]     {relativa}")

    return backup if modificados else None


# ============================================================
# PROGRAMA PRINCIPAL
# ============================================================

def main():

    print("\nSINCRONIZADOR DE REPOSITORIO")
    print("============================")

    # Comprobar rutas

    if not CARPETA_COMPI.exists():

        print("\nERROR:")
        print("No encuentro la carpeta del compañero:")
        print(CARPETA_COMPI)

        sys.exit(1)

    if not MI_REPO.exists():

        print("\nERROR:")
        print("No encuentro tu repositorio:")
        print(MI_REPO)

        sys.exit(1)

    # Comparar

    nuevos, modificados, iguales = comparar()

    mostrar_resultado(
        nuevos,
        modificados,
        iguales
    )

    # Si no hay cambios

    if not nuevos and not modificados:

        print("\nTodo está actualizado.")
        return

    # Preguntar antes de tocar nada

    print("\nTodavía NO se ha modificado ningún archivo.")

    respuesta = input(
        "\n¿Quieres realizar la sincronización? (s/n): "
    )

    if respuesta.lower() != "s":

        print("\nSincronización cancelada.")
        return

    # Sincronizar

    print("\nSincronizando...\n")

    backup = sincronizar(
        nuevos,
        modificados
    )

    print("\n" + "=" * 65)
    print("SINCRONIZACIÓN TERMINADA")
    print("=" * 65)

    if backup:

        print("\nBackup creado en:")
        print(backup)

    print("\nAhora entra en tu repositorio y ejecuta:")
    print()
    print("    git status")
    print()


if __name__ == "__main__":
    main()