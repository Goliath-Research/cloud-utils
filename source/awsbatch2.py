import os
import argparse
import subprocess
import boto3
from botocore.exceptions import ClientError
import re

# Configura el cliente de AWS Batch
batch_client = boto3.client("batch")

default_queue = "preprocessing-default-queue"


# Función para crear el archivo JSON de configuración para AWS Batch trim_galore
def create_trim_galore_json(
    json_path, job_name, job_queue, job_definition, input_file1, input_file2, output_dir
):
    json_text_content = f"""
    {{
        "jobName": "{job_name}",
        "jobQueue": "{job_queue}",
        "jobDefinition": "{job_definition}",
        "containerOverrides": {{
            "command": [
                "trim_galore",
                "--gzip",
                "--phred33",
                "-j 8",
                "--fastqc",
                "--fastqc_args '-t 8'",
                "-o {output_dir}",
                "--paired",
                "{input_file1}",
                "{input_file2}"
            ],
            "environment": []
        }}
    }}
    """

    # Write the JSON file with the correct information
    with open(json_path, "w") as file:
        file.write(json_text_content.strip())


# Función para crear el archivo JSON de configuración para AWS Batch bismark
def create_bismark_json(
    json_path,
    job_name,
    job_queue,
    job_definition,
    input_file1,
    input_file2,
    genome_dir,
    output_dir,
):
    json_text_content = f"""
    {{
        "jobName": "{job_name}",
        "jobQueue": "{job_queue}",
        "jobDefinition": "{job_definition}",
        "containerOverrides": {{
            "command": [
                "bismark --bam --multicore 15 --bowtie2 -p 2 --phred33-quals",
                "--genome_folder {genome_dir}",
                "-o {output_dir}",
                "-1 {input_file1}",
                "-2 {input_file2}"
            ],
            "environment": []
        }}
    }}
    """

    # Write the JSON file with the correct information
    with open(json_path, "w") as file:
        file.write(json_text_content.strip())


# Función para crear el archivo JSON de configuración para AWS Batch bismark
def create_deduplicate_bismark_json(
    json_path, job_name, job_queue, job_definition, bam_file, ouput_dir
):
    json_text_content = f"""
    {{
        "jobName": "{job_name}",
        "jobQueue": "{job_queue}",
        "jobDefinition": "{job_definition}",
        "containerOverrides": {{
            "command": [
                "deduplicate_bismark",
                "-p --bam",
                "--output_dir {ouput_dir}",
                "{bam_file}"
            ],
            "environment": []
        }}
    }}
    """

    # Write the JSON file with the correct information
    with open(json_path, "w") as file:
        file.write(json_text_content.strip())


def create_bismark_methylation_extractor_json(
    json_path,
    job_name,
    job_queue,
    job_definition,
    genome_dir,
    deduplicated_bam_file,
    output_dir,
):
    json_text_content = f"""
    {{
        "jobName": "{job_name}",
        "jobQueue": "{job_queue}",
        "jobDefinition": "{job_definition}",
        "containerOverrides": {{
            "command": [
                "bismark_methylation_extractor",
                "--CX_context --bedGraph --cytosine_report --ample_memory",
                "--multicore 12",
                "--genome_folder {genome_dir}",
                "--gzip",
                "-o {output_dir}",
                "--report {deduplicated_bam_file}"
            ],
            "environment": []
        }}
    }}
    """

    # Write the JSON file with the correct information
    with open(json_path, "w") as file:
        file.write(json_text_content.strip())


# Función para verificar si un archivo existe
def file_exists(filepath):
    return os.path.exists(filepath)


# Función para enviar trabajos a AWS Batch
def submit_job(json_file):
    command = f"aws batch submit-job --cli-input-json file://{json_file}"
    subprocess.run(command, shell=True)


def isfastqgz(filename):
    # Pattern to match either .fq.gz or .fastq.gz
    pattern = r"\.f(ast)?q\.gz$"
    # Check if the filename matches the pattern
    return re.match(pattern, filename) is not None


def replace_extension(filename, ext):
    # Pattern to match either .fq.gz or .fastq.gz
    pattern = r"\.f(ast)?q\.gz$"
    # Replace the matched pattern with the extension
    return re.sub(pattern, ext, filename)


FASTQ_READ1_PATTERN = re.compile(r"^(?P<stem>.+)_1(?P<ext>\.f(?:ast)?q\.gz)$")


# Par de lecturas de una muestra junto con su directorio relativo al
# directorio de datos. Dos muestras con el mismo nombre en directorios
# distintos son muestras distintas, así que cada paso escribe sus resultados
# bajo <step_dir>/<relative_dir>.
class Sample(tuple):
    def __new__(cls, read1, read2, relative_dir):
        sample = super().__new__(cls, (read1, read2))
        sample.relative_dir = relative_dir
        return sample


def sample_output_dir(sample, step_dir):
    return os.path.normpath(os.path.join(step_dir, sample.relative_dir))


def read_stem(path):
    return replace_extension(os.path.basename(path), "")


# Función para obtener las muestras de un directorio dado
# El resultado es una lista de Sample con las dos muestras
# (<muestra>_1.fq.gz y <muestra>_2.fq.gz, o .fastq.gz)
def get_samples(directory):
    samples = []
    for root, _, files in os.walk(directory):
        relative_dir = os.path.relpath(root, directory)
        stems = {}
        for f1 in sorted(files):
            match = FASTQ_READ1_PATTERN.match(f1)
            if match is None:
                continue
            f2 = f"{match.group('stem')}_2{match.group('ext')}"
            if f2 not in files:
                continue
            # trim_galore names its outputs after the read without its
            # extension, so X_1.fq.gz and X_1.fastq.gz would share outputs.
            stem = match.group("stem")
            if stem in stems:
                raise ValueError(
                    f"Samples {stems[stem]} and {f1} in {root} would produce "
                    "the same output files"
                )
            stems[stem] = f1
            samples.append(
                Sample(os.path.join(root, f1), os.path.join(root, f2), relative_dir)
            )
    return samples


def create_trim_galore_outputs(sample, output_dir):
    sample1, sample2 = sample
    output1path = os.path.join(output_dir, f"{read_stem(sample1)}_val_1.fq.gz")
    output2path = os.path.join(output_dir, f"{read_stem(sample2)}_val_2.fq.gz")
    return output1path, output2path


# Paso 1: Ejecuta trim_galore para cada muestra
def execute_trim_galore(samples, config_dir, output_dir):
    step_dir = f"{output_dir}/out_trim"
    i = 0
    for sample in samples:
        sample1, sample2 = sample
        output_dir = sample_output_dir(sample, step_dir)
        # Verifica si los archivos de salida de trim_galore ya existen
        output1, output2 = create_trim_galore_outputs(sample, output_dir)
        if not file_exists(output1) or not file_exists(output2):
            i += 1
            json_path = os.path.join(config_dir, f"{i}-trim_galore.json")
            create_trim_galore_json(
                json_path=json_path,
                job_name=f"trim_galore{i}",
                job_queue=default_queue,
                job_definition=f"methylit-trim_galore:{i}",
                input_file1=sample1,
                input_file2=sample2,
                output_dir=output_dir,
            )
            submit_job(json_path)
            print(f"Submitted trim_galore job {i} for {sample1} and {sample2}")


def create_bismark_output(sample, output_dir):
    sample1, _ = sample
    return os.path.join(output_dir, f"{read_stem(sample1)}_val_1_bismark_bt2_pe.bam")


# Paso 2: Ejecuta bismark para cada muestra procesada en trim_galore
def execute_bismark(samples, config_dir, genome_dir, output_dir):
    step_dir = f"{output_dir}/out_bismark"
    i = 0
    for sample in samples:
        sample1, sample2 = sample
        output_dir = sample_output_dir(sample, step_dir)
        # Verifica si los archivos de salida de bismark ya existen
        bam_output = create_bismark_output(sample, output_dir)
        if not file_exists(bam_output):
            i += 1
            json_path = os.path.join(config_dir, f"{i}-bismark.json")
            create_bismark_json(
                json_path=json_path,
                job_name=f"bismark{i}",
                job_queue=default_queue,
                job_definition=f"methylit-bismark:{i}",
                input_file1=sample1,
                input_file2=sample2,
                genome_dir=genome_dir,
                output_dir=output_dir,
            )
            submit_job(json_path)
            print(f"Submitted bismark job {i} for {sample1} and {sample2}")


def create_deduplicate_bismark_output(sample, output_dir):
    sample1, _ = sample
    return os.path.join(
        output_dir, f"{read_stem(sample1)}_val_1_bismark_bt2_pe.deduplicated.bam"
    )


# Paso 3: Ejecuta deduplicate_bismark para los archivos BAM generados
def execute_deduplicate_bismark(samples, config_dir, output_dir):
    input_step_dir = f"{output_dir}/out_bismark"
    step_dir = f"{output_dir}/out_deduplicate"
    i = 0
    for sample in samples:
        output_dir = sample_output_dir(sample, step_dir)
        bam_file = create_bismark_output(
            sample, sample_output_dir(sample, input_step_dir)
        )
        deduplicated_bam = create_deduplicate_bismark_output(sample, output_dir)
        if not file_exists(deduplicated_bam):
            i += 1
            json_path = os.path.join(config_dir, f"{i}-deduplicate_bismark.json")
            create_deduplicate_bismark_json(
                json_path=json_path,
                job_name=f"deduplicate_bismark{i}",
                job_queue=default_queue,
                job_definition=f"methylit-deduplicate_bismark:{i}",
                bam_file=bam_file,
                ouput_dir=output_dir,
            )
            submit_job(json_path)
            print(f"Submitted deduplicate_bismark job {i} for {bam_file}")


def create_bismark_methylation_extractor_output(deduplicated_bam_file, output_dir):
    bam_name = os.path.basename(deduplicated_bam_file)
    report_name = re.sub(r"\.bam$", ".CX_report.txt.gz", bam_name)
    return os.path.join(output_dir, report_name)


# Paso 4: Ejecuta bismark_methylation_extractor para el archivo BAM final
def execute_bismark_methylation_extractor(samples, config_dir, genome_dir, output_dir):
    input_step_dir = f"{output_dir}/out_deduplicate"
    step_dir = f"{output_dir}/out_report"
    i = 0
    for sample in samples:
        output_dir = sample_output_dir(sample, step_dir)
        deduplicated_bam_file = create_deduplicate_bismark_output(
            sample, sample_output_dir(sample, input_step_dir)
        )
        methylation_output = create_bismark_methylation_extractor_output(
            deduplicated_bam_file, output_dir
        )
        if not file_exists(methylation_output):
            i += 1
            json_path = os.path.join(
                config_dir, f"{i}-bismark_methylation_extractor.json"
            )
            create_bismark_methylation_extractor_json(
                json_path=json_path,
                job_name=f"bismark_methylation_extractor{i}",
                job_queue=default_queue,
                job_definition=f"methylit-bismark_methylation_extractor:{i}",
                genome_dir=genome_dir,
                deduplicated_bam_file=deduplicated_bam_file,
                output_dir=output_dir,
            )
            submit_job(json_path)
            print(
                f"Submitted bismark_methylation_extractor job {i} for {deduplicated_bam_file}"
            )


# Paso 5: Copia los archivos CX_report.txt.gz a S3
def upload_new_reports_to_S3(output_dir, s3_bucket):
    output_dir = f"{output_dir}/out_report"
    s3_client = boto3.client("s3")
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".CX_report.txt.gz"):
                local_file_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_file_path, output_dir)

                try:
                    s3_response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
                    s3_last_modified = s3_response["LastModified"].timestamp()
                    local_last_modified = os.path.getmtime(local_file_path)

                    if local_last_modified > s3_last_modified:
                        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                        print(f"Uploaded: {local_file_path}")
                except ClientError as e:
                    if e.response["Error"]["Code"] == "404":
                        s3_client.upload_file(local_file_path, s3_bucket, s3_key)
                        print(f"Uploaded: {local_file_path}")
                    else:
                        print(f"Error checking {s3_key}: {e}")


def main():
    # Create the argument parser
    parser = argparse.ArgumentParser(
        description="Process a directory of genome data to another directory."
    )

    # Add arguments for job name and job queue
    parser.add_argument(
        "--data_dir",
        type=str,
        help="The data directory to process",
        default="/data/test_jobs/rawdata",
    )
    parser.add_argument(
        "--config_dir",
        type=str,
        help="The config directory for the JSON files",
        default="/home/ubuntu/environment/test-jobs/preprocess",
    )
    parser.add_argument(
        "--genome_dir",
        type=str,
        help="The genome directory for Bismark processing",
        default="/data/genomes/GRCh38",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        help="The output directory for the processed data",
        default="/data/gladys",
    )
    parser.add_argument(
        "--s3_bucket",
        type=str,
        help="The s3 bucket to copy the results to",
        default="gladys-results",
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    samples = get_samples(args.data_dir)
    # Execute the main program (all processing steps)
    execute_trim_galore(samples, args.config_dir, args.output_dir)
    execute_bismark(samples, args.config_dir, args.genome_dir, args.output_dir)
    execute_deduplicate_bismark(samples, args.config_dir, args.output_dir)
    execute_bismark_methylation_extractor(
        samples, args.config_dir, args.genome_dir, args.output_dir
    )
    upload_new_reports_to_S3(args.output_dir, args.s3_bucket)


if __name__ == "__main__":
    main()
