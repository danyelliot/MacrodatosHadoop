# MacrodatosHadoop

Implementación de 12 consultas MapReduce con Apache Hadoop sobre el dataset **Cobertura de Programas Sociales MIDIS (Diciembre 2024)** de la Plataforma Nacional de Datos Abiertos del Perú.

**Curso:** CC531 A - Análisis en Macrodatos | Universidad Nacional de Ingeniería  
**Laboratorio:** 01 | 2026

---

## Dataset

**Cobertura de los Programas Sociales adscritos al MIDIS**  
Fuente: [datosabiertos.gob.pe](https://datosabiertos.gob.pe/dataset/cobertura-de-los-programas-sociales-adscritos-al-midis)  
Formato: CSV con separador `;` | 1,892 registros (distritos) | 18 columnas  
Programas: JUNTOS, PENSIÓN 65, QALI WARMA, CUNAMAS, FONCODES, CONTIGO, PAIS

---

## Requisitos

- Apache Hadoop 3.3.6
- Java 11 (Eclipse Temurin 11.0.30 recomendado)
- macOS / Linux

> Hadoop 3.3.x **no es compatible con Java 18 o superior**. Usar Java 11 o 17.

---

## Configuración de Hadoop

Variables de entorno en `~/.zshrc`:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home
export HADOOP_HOME=/ruta/a/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

---

## Compilar y empaquetar una consulta

```bash
# Variables
BASE="/ruta/a/MacrodatosHadoop"
HADOOP_HOME="/ruta/a/hadoop-3.3.6"
HADOOP_CP=$(find $HADOOP_HOME/share/hadoop/common $HADOOP_HOME/share/hadoop/mapreduce -name "*.jar" | tr '\n' ':')

# Reemplazar XX por el número de consulta (01, 02, ..., 12)
N=01

mkdir -p $BASE/consulta$N/classes

javac -classpath "$HADOOP_CP" \
  -d $BASE/consulta$N/classes \
  $BASE/consulta$N/src/SalesCountry/*.java

cd $BASE/consulta$N/classes

jar -cvfe $BASE/out/artifacts/consulta${N}_jar/consulta$N.jar \
  SalesCountry.SalesCountryDriver \
  SalesCountry/
```

---

## Ejecutar en Hadoop

```bash
# Subir el dataset a HDFS (solo la primera vez)
hadoop fs -mkdir /midis
hadoop fs -put DICIEMBRE_2024.csv /midis/

# Ejecutar una consulta
hadoop jar out/artifacts/consulta01_jar/consulta01.jar SalesCountry.SalesCountryDriver

# Ver el resultado
hadoop fs -cat "/output_c01/*"

# Limpiar output antes de volver a ejecutar
hadoop fs -rm -r /output_c01
```

---

## Consultas implementadas

| # | Consulta | Tipo |
|---|----------|------|
| 01 | Total hogares JUNTOS (afiliados + abonados) por departamento | 2 campos |
| 02 | Total usuarios PENSIÓN 65 y CONTIGO por departamento | 2 campos |
| 03 | Distritos con CUNAMAS activo por departamento | 2 campos |
| 04 | Niños atendidos e IIEE de QALI WARMA por departamento | 2 campos |
| 05 | Mayor y menor cobertura JUNTOS por departamento | Mayor/menor |
| 06 | Promedio, mediana y desviación estándar de PENSIÓN 65 | Estadísticas |
| 07 | Búsqueda de registros por subtexto en UBIGEO | Búsqueda texto |
| 08 | Registros filtrados por intervalo de fechas | Intervalo fechas |
| 09 | Ratio hogares abonados/afiliados JUNTOS | 2 MapReduce encadenados |
| 10 | Ratio niños/IIEE de QALI WARMA | 2 MapReduce encadenados |
| 11 | Clasificación de distritos por nivel de cobertura PENSIÓN 65 | Clasificación |
| 12 | Regresión lineal de usuarios PENSIÓN 65 por distrito | Regresión lineal |

---

## Estructura del proyecto

```
MacrodatosHadoop/
├── consulta01/
│   └── src/SalesCountry/
│       ├── SalesMapper.java
│       ├── SalesCountryReducer.java
│       └── SalesCountryDriver.java
├── consulta02/ ... consulta12/  (misma estructura)
└── out/artifacts/
    ├── consulta01_jar/consulta01.jar
    └── ... (un JAR por consulta)
```
