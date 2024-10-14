import os
import subprocess
import boto3

# Configura el cliente de AWS Batch
batch_client = boto3.client('batch')

# Define las rutas de los archivos JSON de configuración
config_path = "/home/ubuntu/environment/gladys/preprocess/"
trim_galore_json = os.path.join(config_path, "01-trim_galore.json")
bismark_json = os.path.join(config_path, "02-bismark.json")
deduplicate_bismark_json = os.path.join(config_path, "03-deduplicate_bismark.json")
methylation_extractor_json = os.path.join(config_path, "04-bismark_methylation_extractor.json")

# Define el directorio base de los datos
# Para mayor flexibilidad, los datos se pueden almacenar en cualquier cantidad
# de niveles en la estructura de directorios
base_dir = "/data/hardik/rawdata/usftp21.novogene.com/01.RawData/"

# El diseño de este programa asume que los datos y la mayoría de sus resultados
# estarán en los directorios bajo el <base_dir>, lo cual permitirá recuperar y
# continuar el procesamiento en caso de cualquier fallo.
# Sin embargo, eso también indica que, una vez terminado un procesamiento, los
# datos deberán moverse a otro directorio <01.ProcessedData> o similar y que
# cualquier nuevo conjunto de datos a procesar se subirían al mismo lugar.
# Otra opción es crear un directorio de datos para cada procesamiento, porque
# la única ubicación que compartiría un procesamiento sería el archivo único
# de resultados.
# Un diseño óptimo sería pasar el <base_dir> y <output_dir> como parámetros al
# programa cada vez que se ejecutara (o modificar variables de environment).
# Eso garantizaría un ambiente único para cada conjunto de datos con sus resultados
# y evitaría el movimiento de los datos o resultados (que suele ser rápido en S3)

# Función para verificar si un archivo existe
def file_exists(filepath):
    return os.path.isfile(filepath)

# Función para enviar trabajos a AWS Batch
def submit_job(json_file):
    command = f"aws batch submit-job --cli-input-json file://{json_file}"
    subprocess.run(command, shell=True)

# Función para obtener las muestras de un directorio dado
# El resultado es una lista de tuplas con las dos muestras
# (<muestra>_1.fq.gz y <muestra>_2.fq.gz)
def get_samples(directory):
    samples = []
    for root, _, files in os.walk(directory):
        fq_files_1 = [f for f in files if f.endswith("_1.fq.gz")]
        for f1 in fq_files_1:
            f2 = f1.replace("_1.fq.gz", "_2.fq.gz")
            if f2 in files:
                samples.append((os.path.join(root, f1), os.path.join(root, f2)))
    return samples

# Paso 1: Ejecuta trim_galore para cada muestra
# Una vez procesada cada muestra (<muestra>_1.fq.gz, <muestra>_2.fq.gz),
# se crean dos resultados (<muestra>_val_1.fq.gz, <muestra>_val_2.fq.gz)
samples = get_samples(base_dir)
for sample1, sample2 in samples:
    # Verifica si los archivos de salida de trim_galore ya existen
    output1 = sample1.replace(".fq.gz", "_val_1.fq.gz")
    output2 = sample2.replace(".fq.gz", "_val_2.fq.gz")
    if not file_exists(output1) or not file_exists(output2):
        submit_job(trim_galore_json)
        print(f"Submitted trim_galore job for {sample1} and {sample2}")

# Paso 2: Ejecuta bismark para cada muestra procesada en trim_galore
for sample1, sample2 in samples:
    output1 = sample1.replace(".fq.gz", "_val_1.fq.gz")
    output2 = sample2.replace(".fq.gz", "_val_2.fq.gz")
    # Verifica si los archivos de salida de bismark ya existen
    bam_output = sample1.replace(".fq.gz", ".bam")
    if not file_exists(bam_output):
        submit_job(bismark_json)
        print(f"Submitted bismark job for {output1} and {output2}")

# Paso 3: Ejecuta deduplicate_bismark para los archivos BAM generados
bam_files = [f.replace(".fq.gz", ".bam") for f, _ in samples]
deduplicated_bam = "/data/gladys/out_report/fichero_general.bam"
if not file_exists(deduplicated_bam):
    submit_job(deduplicate_bismark_json)
    print("Submitted deduplicate_bismark job for BAM files")

# Paso 4: Ejecuta bismark_methylation_extractor para el archivo BAM final
methylation_output = "/data/gladys/out_report/NOMBRE_FICHERO.CX_report.txt.gz"
if not file_exists(methylation_output):
    submit_job(methylation_extractor_json)
    print("Submitted bismark_methylation_extractor job")

# Paso 5: Copia el archivo CX_report.txt.gz a S3
s3_bucket = "s3://rawdata-samples/reportes/"
command = f"aws s3 cp {methylation_output} {s3_bucket}"
subprocess.run(command, shell=True)
print(f"Copied {methylation_output} to S3 bucket {s3_bucket}")
