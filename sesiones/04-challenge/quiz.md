# Quiz · Knowledge Check

⏱️ **7 minutos.** Una respuesta por pregunta. Respuestas: [soluciones/quiz_respuestas.md](soluciones/quiz_respuestas.md).

**1. ¿Qué imprime este código?**
```python
def agregar(x, lista=[]):
    lista.append(x)
    return lista
agregar(1); print(agregar(2))
```
a) `[2]` · b) `[1, 2]` · c) Error · d) `[[1], 2]`

**2. En PySpark, ¿cuál de estas operaciones dispara la ejecución?**
a) `filter` · b) `withColumn` · c) `groupBy().agg()` · d) `count()`

**3. Una tabla de dimensión tiene llaves duplicadas y haces un `left join` desde la tabla de hechos. ¿Qué pasa?**
a) Spark lanza un error · b) Las filas de hechos se multiplican sin error · c) Spark deduplica automáticamente · d) Nada, el left join lo evita

**4. ¿Qué función usarías para quedarte con el último registro por cliente de forma determinística?**
a) `dropDuplicates(["cliente"])` · b) `groupBy("cliente").agg(F.max("fecha"))` · c) `row_number()` sobre una window con orden explícito · d) `distinct()`

**5. ¿Por qué BPT prefiere funciones built-in (`F.*`) sobre UDFs de Python?**
a) Las UDF no funcionan en Databricks · b) Las built-in corren en la JVM y el optimizador las entiende · c) Las UDF no aceptan nulos · d) Por estilo, no hay diferencia de rendimiento

**6. Reejecutas un proceso que escribe con `mode("append")`. ¿Qué pasa?**
a) Nada, Delta lo detecta · b) Se duplican los datos · c) Falla por schema · d) Se sobrescribe la tabla

**7. ¿Qué comando muestra quién cambió una tabla Delta, cuándo y con qué operación?**
a) `DESCRIBE DETAIL` · b) `SHOW TABLES` · c) `DESCRIBE HISTORY` · d) `SELECT * FROM … VERSION AS OF 0`

**8. ¿En qué capa Medallion se aplican las reglas de calidad y la deduplicación?**
a) Bronze · b) Silver · c) Gold · d) En todas por igual

**9. Un registro no cumple una regla de calidad. Según la guía BPT, ¿qué se hace con él?**
a) Se descarta · b) Se corrige a mano en Silver · c) Se envía a una tabla de quarantine con las reglas incumplidas · d) Se deja pasar con una advertencia en el log

**10. ¿Dónde debe vivir la lógica de transformación reutilizable?**
a) En celdas del notebook · b) En módulos `.py` de `src/`, importados desde los notebooks · c) En una UDF registrada · d) En la tabla de log
