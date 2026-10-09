# 06 · Calidad y seguridad del código

Reglas que se verifican automáticamente en cada Pull Request antes de integrar código.

---

## 1. Quality Gate: SonarCloud y Fluid Attacks

- **SonarCloud** analiza cada PR (análisis estático de calidad y seguridad, SAST) antes de integrarlo a las ramas de desarrollo y release. **Todo PR debe pasar el Quality Gate.**
- **Fluid Attacks** complementa con análisis de seguridad más profundo. Si detecta vulnerabilidades altas o críticas, el pipeline de validación falla.
- Son complementarios: ninguno reemplaza al otro.

Los hallazgos se clasifican según estándares reconocidos: OWASP Top 10, CWE Top 25 y OWASP ASVS.

### 1.1 Tipos de hallazgo

| Tipo | Qué es | Qué hacer |
|---|---|---|
| **Vulnerability** | Falla de seguridad explotable (inyección SQL, credenciales en código, `eval`) | Corregir antes del merge. No se marca "Won't Fix" sin aprobación del arquitecto de datos |
| **Security Hotspot** | Código sensible que requiere revisión manual | Clasificar como Safe, Fixed o Acknowledged antes de promover a QA |
| **Bug** | Defecto que causa un comportamiento incorrecto | Corregir |
| **Code Smell** | Problema de mantenibilidad | Los de severidad mayor llevan un plan de remediación documentado |

No se permiten excepciones para hallazgos **Blocker** o **Critical**.

---

## 2. Credenciales y secretos

**Regla:** ninguna credencial, token, contraseña o cadena de conexión en código, notebooks o repositorios. Todo se resuelve con `dbutils.secrets.get()`, vinculado al gestor de secretos de la plataforma (por ejemplo, Azure Key Vault).

```python
# ❌ Bloqueante
password = "MySecretPass123"
connection_string = "Server=myserver;Password=secret"

# ✅
password = dbutils.secrets.get(scope="<scope>", key="<key>")
```

---

## 3. Ejecución dinámica de código

### 3.1 Sin `eval()` ni `exec()`
No se permiten en código productivo porque pueden ejecutar código arbitrario.

```python
# ❌ eval sobre texto que viene de configuración o metadata
settings = eval(raw_settings)

# ✅
import json

settings = json.loads(raw_settings)
```

### 3.2 Funciones dinámicas desde un registro controlado
Cuando la metadata indica qué transformaciones aplicar, se usa un diccionario de funciones permitidas, nunca `exec()` sobre un texto.

```python
UDF_REGISTRY = {
    "_udf_clean_columns": _udf_clean_columns,
    "_udf_normalize_column_names": _udf_normalize_column_names,
}

for udf_name in nb_udfs.split("|"):
    udf_function = UDF_REGISTRY.get(udf_name.strip())
    if udf_function is None:
        raise ValueError(f"UDF no registrada: {udf_name}")
    df_clean = udf_function(df_clean)
```

### 3.3 SQL con parámetros validados
Antes de usar un parámetro dentro de una consulta, se valida contra los valores permitidos.

```python
# ❌ Concatenación directa: riesgo de inyección
query = "SELECT * FROM config WHERE system = '" + user_input + "'"

# ✅ Valor validado contra una lista permitida
ALLOWED_SYSTEMS = {"FLIGHTS", "BOOKINGS"}
if wf_system not in ALLOWED_SYSTEMS:
    raise ValueError(f"Sistema no permitido: {wf_system}")
df_meta = spark.sql(
    "SELECT * FROM config WHERE system = :system",
    args={"system": wf_system},
)
```

---

## 4. Datos sensibles en logs

**Regla:** los logs y los `print` no muestran credenciales ni datos personales (nombres, documentos, correos).

```python
# ❌ Expone datos personales y credenciales en los logs del job
print(f"Conectando con password: {password}")
print(f"Pasajero: {name}, documento: {document_number}")

# ✅ Información operativa, sin datos sensibles
logger.info("Reservas procesadas: %s", booking_count)
```

---

## 5. Errores silenciados

Bloquear la detección de errores también es un hallazgo de calidad. Ver [01 §6](01-python.md#6-manejo-de-errores).

```python
# ❌ Prohibido
try:
    df_final.write.saveAsTable(target_table)
except Exception:
    print("algo falló")
```
