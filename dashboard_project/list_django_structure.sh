#!/bin/bash

# Verifica si se proporcionó un directorio como argumento
if [ $# -eq 0 ]; then
    # Si no se proporciona directorio, usa el directorio actual
    PROJECT_DIR="."
else
    PROJECT_DIR="$1"
fi

# Nombre del archivo de salida
OUTPUT_FILE="project_structure.txt"

# Crea el archivo y escribe el encabezado
echo "Estructura del Proyecto Django" > "$OUTPUT_FILE"
echo "=============================" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Usa tree si está instalado, de lo contrario usa find
if command -v tree >/dev/null 2>&1; then
    # Excluye directorios y archivos comunes que no son necesarios
    tree -a -I 'venv|*.pyc|__pycache__|.git|.DS_Store' "$PROJECT_DIR" >> "$OUTPUT_FILE"
else
    # Si tree no está instalado, usa find como alternativa
    find "$PROJECT_DIR" -not -path '*/\.*' -not -path '*/venv/*' -not -name '*.pyc' -not -name '.DS_Store' | \
    sed -e "s/[^-][^\/]*\// |--/g" -e "s/|-|/ |-/g" >> "$OUTPUT_FILE"
fi

# Mensaje de confirmación
echo "La estructura del proyecto ha sido guardada en $OUTPUT_FILE"