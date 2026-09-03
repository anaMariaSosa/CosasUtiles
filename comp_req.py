from pathlib import Path
import subprocess
import sys
import re

# ============================================================
# CONFIGURACIÓN
# ============================================================

REQUIREMENTS_COMPI = Path(
    r"Z:\ruta\requirements_ultimo.txt"
)

MI_REQUIREMENTS = Path(
    r"C:\ruta\mi_repo\requirements.txt"
)

PYTHON_VENV = Path(
    r"C:\ruta\mi_repo\venv\Scripts\python.exe"
)


# ============================================================
# LEER REQUIREMENTS
# ============================================================

def normalizar_nombre(nombre):
    return nombre.lower().replace("_", "-").strip()


def leer_requirements(path):
    paquetes = {}

    with open(path, "r", encoding="utf-8") as f:
        for linea in f:

            linea = linea.strip()

            if not linea or linea.startswith("#"):
                continue

            # Soporta sobre todo formatos tipo:
            # pandas==2.2.3
            # numpy>=2.0.0
            # requests

            match = re.match(
                r"^([A-Za-z0-9_.\-]+)\s*(==|>=|<=|~=|>|<)?\s*(.*)$",
                linea
            )

            if not match:
                continue

            nombre = normalizar_nombre(match.group(1))
            operador = match.group(2) or ""
            version = match.group(3).strip()

            paquetes[nombre] = {
                "operador": operador,
                "version": version,
                "linea": linea
            }

    return paquetes


# ============================================================
# LEER VENV REAL
# ============================================================

def leer_venv():

    if not PYTHON_VENV.exists():
        raise FileNotFoundError(
            f"No encuentro el Python del venv:\n{PYTHON_VENV}"
        )

    resultado = subprocess.run(
        [
            str(PYTHON_VENV),
            "-m",
            "pip",
            "freeze"
        ],
        capture_output=True,
        text=True,
        check=True
    )

    paquetes = {}

    for linea in resultado.stdout.splitlines():

        linea = linea.strip()

        if "==" not in linea:
            continue

        nombre, version = linea.split("==", 1)

        paquetes[
            normalizar_nombre(nombre)
        ] = version.strip()

    return paquetes


# ============================================================
# INFORME
# ============================================================

def mostrar_valor(datos):

    if datos is None:
        return "--------"

    operador = datos["operador"]
    version = datos["version"]

    if version:
        return f"{operador}{version}"

    return "sin versión"


def main():

    if not REQUIREMENTS_COMPI.exists():
        print("ERROR: no encuentro requirements del compi:")
        print(REQUIREMENTS_COMPI)
        return

    if not MI_REQUIREMENTS.exists():
        print("ERROR: no encuentro mi requirements:")
        print(MI_REQUIREMENTS)
        return

    try:
        venv = leer_venv()

    except Exception as e:
        print("ERROR leyendo el venv:")
        print(e)
        return

    compi = leer_requirements(REQUIREMENTS_COMPI)
    local = leer_requirements(MI_REQUIREMENTS)

    # Unión de todas las librerías conocidas
    librerias = sorted(
        set(compi.keys()) |
        set(local.keys()) |
        set(venv.keys())
    )

    print()
    print("=" * 100)
    print(
        f"{'LIBRERÍA':<25}"
        f"{'REQUIREMENTS COMPI':<25}"
        f"{'MI REQUIREMENTS':<25}"
        f"{'MI VENV':<20}"
        f"ESTADO"
    )
    print("=" * 100)

    for nombre in librerias:

        datos_compi = compi.get(nombre)
        datos_local = local.get(nombre)
        version_venv = venv.get(nombre)

        texto_compi = mostrar_valor(datos_compi)
        texto_local = mostrar_valor(datos_local)
        texto_venv = version_venv or "--------"

        # ----------------------------------------------------
        # ESTADO
        # ----------------------------------------------------

        if datos_compi is None:

            if datos_local is not None or version_venv is not None:
                estado = "SOLO LOCAL"

            else:
                estado = "OK"

        elif datos_local is None:

            estado = "FALTA REQUIREMENT"

        elif version_venv is None:

            estado = "NO INSTALADA"

        else:

            version_compi = datos_compi["version"]
            version_local = datos_local["version"]

            if (
                datos_compi["operador"] == "=="
                and datos_local["operador"] == "=="
            ):

                if (
                    version_compi == version_local
                    and version_local == version_venv
                ):
                    estado = "OK"

                elif version_compi != version_local:
                    estado = "REQUIREMENT DISTINTO"

                elif version_local != version_venv:
                    estado = "VENV DISTINTO"

                else:
                    estado = "REVISAR"

            else:
                # Si hay >=, <=, ~= etc., no hacemos una evaluación
                # semántica todavía.
                estado = "REVISAR RANGO"

        print(
            f"{nombre:<25}"
            f"{texto_compi:<25}"
            f"{texto_local:<25}"
            f"{texto_venv:<20}"
            f"{estado}"
        )

    print("=" * 100)


if __name__ == "__main__":
    main()

# EJEMPLO DE OUTPUT	
# ==============================================================================================================================
# LIBRERÍA                 REQUIREMENTS COMPI       MI REQUIREMENTS          MI VENV             ESTADO
# ====================================================================================================
# ipykernel                 ==6.30.1                 ==6.30.1                 6.30.1              OK
# numpy                     ==2.1.3                  ==2.1.3                  2.1.3               OK
# openpyxl                  ==3.1.5                  --------                 --------            FALTA REQUIREMENT
# pandas                    ==2.3.0                  ==2.2.3                  2.2.3               REQUIREMENT DISTINTO
# requests                  ==2.32.4                 ==2.32.4                 --------            NO INSTALADA
# scikit-learn              --------                 ==1.7.0                  1.7.0               SOLO LOCAL
# ==============================================================================================================================
# LEYENDA:
# OK → los tres coinciden.
# FALTA REQUIREMENT → tu compi la necesita pero tú ni la declaras.
# NO INSTALADA → está en requirements pero falta realmente en tu venv.
# REQUIREMENT DISTINTO → tu requirements.txt lleva otra versión.
# VENV DISTINTO → tu requirements está bien, pero tienes instalada otra versión.
# SOLO LOCAL → tú la tienes pero el último requirements del compi no.
# REVISAR RANGO → hay algo tipo pandas>=2.0, que requiere comprobar compatibilidad en vez de igualdad exacta.