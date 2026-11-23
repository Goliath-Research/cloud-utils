"""Sequencing outputs must follow the sample's directory, not its base name.

`source/awsbatch2.py` walks every subdirectory of the input dataset. Two read
pairs with the same file names under different directories are different
samples. Each later step has to keep that directory in the output it checks
for, the output directory it hands to the tool, and the object key it uploads.

These tests fail while those paths are built from the base name only. A run
that still submits the job, but points the tool at the shared base-name path
after checking a different path, fails too: the path that is checked is the
path the tool is told to write.

The tool still names its file from the input file's base name. The directory
that contains that file is what has to carry the sample path relative to the
input dataset (`batch_one/cohort_a` versus `batch_two/cohort_a` here, the same
final folder name on purpose).
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source"))

os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import awsbatch2
from botocore.exceptions import ClientError

REL_ONE = Path("batch_one") / "cohort_a"
REL_TWO = Path("batch_two") / "cohort_a"
READ_ONE = "SAMP_1.fastq.gz"
READ_TWO = "SAMP_2.fastq.gz"
BUCKET = "results-bucket"

# File names the tools derive from the read base name. Placing these directly
# in the shared step directory is the collision these tests reject.
FLAT_OUTPUTS = {
    "trim": (
        "out_trim",
        (
            ("SAMP_1_val_1.fq.gz", b"flat-trim-r1"),
            ("SAMP_2_val_2.fq.gz", b"flat-trim-r2"),
        ),
    ),
    "bismark": (
        "out_bismark",
        (("SAMP_1_val_1_bismark_bt2_pe.bam", b"flat-bam"),),
    ),
    "dedup": (
        "out_deduplicate",
        (("SAMP_1_val_1_bismark_bt2_pe.deduplicated.bam", b"flat-dedup"),),
    ),
    "methylation": (
        "out_report",
        (
            (
                "SAMP_1_val_1_bismark_bt2_pe.deduplicated.CX_report.txt.gz",
                b"flat-report",
            ),
        ),
    ),
}


def _norm(path):
    return os.path.normpath(path)


def _contains_relative(path, relative):
    parts = Path(path).parts
    wanted = relative.parts
    span = len(wanted)
    return any(parts[index : index + span] == wanted for index in range(len(parts) - span + 1))


def _command(job):
    return job["containerOverrides"]["command"]


def _output_dir(job):
    for part in _command(job):
        for prefix in ("-o ", "--output_dir "):
            if part.startswith(prefix):
                return part[len(prefix) :]
    raise AssertionError("job command has no output directory: %r" % (_command(job),))


def _input_paths(stage, job):
    command = _command(job)
    if stage == "trim":
        return [command[-2], command[-1]]
    if stage == "bismark":
        return [
            part.split(" ", 1)[1]
            for part in command
            if part.startswith("-1 ") or part.startswith("-2 ")
        ]
    if stage == "dedup":
        return [command[-1]]
    if stage == "methylation":
        return [
            part.split(" ", 1)[1] for part in command if part.startswith("--report ")
        ]
    raise AssertionError(stage)


def _marker(paths):
    found = []
    for relative in (REL_ONE, REL_TWO):
        token = str(relative)
        if any(token in path for path in paths):
            found.append(relative)
    return found


def _expected_outputs(stage, job):
    """Files the step writes inside the output directory it passes to the tool."""
    output_dir = _output_dir(job)
    inputs = _input_paths(stage, job)
    if stage == "trim":
        read1, read2 = inputs
        return [
            os.path.join(
                output_dir,
                os.path.basename(read1).replace(".fastq.gz", "_val_1.fq.gz"),
            ),
            os.path.join(
                output_dir,
                os.path.basename(read2).replace(".fastq.gz", "_val_2.fq.gz"),
            ),
        ]
    if stage == "bismark":
        read1 = inputs[0]
        return [
            os.path.join(
                output_dir,
                os.path.basename(read1).replace(
                    ".fastq.gz", "_val_1_bismark_bt2_pe.bam"
                ),
            )
        ]
    if stage == "dedup":
        bam_name = os.path.basename(inputs[0])
        if not bam_name.endswith(".bam"):
            raise AssertionError(bam_name)
        return [os.path.join(output_dir, bam_name[: -len(".bam")] + ".deduplicated.bam")]
    if stage == "methylation":
        bam_name = os.path.basename(inputs[0])
        if not bam_name.endswith(".bam"):
            raise AssertionError(bam_name)
        return [
            os.path.join(
                output_dir, bam_name[: -len(".bam")] + ".CX_report.txt.gz"
            )
        ]
    raise AssertionError(stage)


class _RecordingS3:
    """Records HeadObject and upload_file arguments.

    `head_object` raises the same ClientError shape boto3/botocore uses for a
    missing object: `response["Error"]["Code"] == "404"`. That is the branch
    `upload_new_reports_to_S3` handles by uploading. No request leaves the
    process.
    """

    def __init__(self):
        self.heads = []
        self.uploads = []

    def head_object(self, Bucket, Key):
        self.heads.append({"bucket": Bucket, "key": Key})
        raise ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

    def upload_file(self, Filename, Bucket, Key, ExtraArgs=None, Callback=None, Config=None):
        self.uploads.append(
            {
                "filename": Filename,
                "bucket": Bucket,
                "key": Key,
                "body": Path(Filename).read_bytes(),
            }
        )


class OutputIdentityTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self.data_dir = root / "data"
        self.output_dir = root / "output"
        self.config_dir = root / "config"
        self.genome_dir = root / "genome"
        for directory in (
            self.data_dir,
            self.output_dir,
            self.config_dir,
            self.genome_dir,
        ):
            directory.mkdir()
        self._write_pair(REL_ONE, b"alpha")
        self._write_pair(REL_TWO, b"beta")
        self.samples = awsbatch2.get_samples(str(self.data_dir))
        sample_paths = [path for pair in self.samples for path in pair]
        self.assertEqual(len(self.samples), 2)
        self.assertTrue(any(str(REL_ONE) in path for path in sample_paths))
        self.assertTrue(any(str(REL_TWO) in path for path in sample_paths))
        self.events = []
        self._real_exists = awsbatch2.file_exists
        self._real_submit = awsbatch2.submit_job
        self._real_client = awsbatch2.boto3.client

        def spy_exists(path):
            present = self._real_exists(path)
            self.events.append(("exists", path, present))
            return present

        def spy_submit(json_path):
            job = json.loads(Path(json_path).read_text())
            self.events.append(("submit", job, json_path))

        awsbatch2.file_exists = spy_exists
        awsbatch2.submit_job = spy_submit
        self.addCleanup(self._restore_module)

    def _restore_module(self):
        awsbatch2.file_exists = self._real_exists
        awsbatch2.submit_job = self._real_submit
        awsbatch2.boto3.client = self._real_client

    def _write_pair(self, relative, body):
        directory = self.data_dir / relative
        directory.mkdir(parents=True)
        (directory / READ_ONE).write_bytes(body + b"-r1")
        (directory / READ_TWO).write_bytes(body + b"-r2")

    def _reset_run(self):
        self.events = []
        for folder in (self.output_dir, self.config_dir):
            for child in folder.iterdir():
                if child.is_file() or child.is_symlink():
                    child.unlink()
                else:
                    shutil.rmtree(child)

    def _run(self, stage):
        samples = self.samples
        config = str(self.config_dir)
        output = str(self.output_dir)
        genome = str(self.genome_dir)
        if stage == "trim":
            awsbatch2.execute_trim_galore(samples, config, output)
        elif stage == "bismark":
            awsbatch2.execute_bismark(samples, config, genome, output)
        elif stage == "dedup":
            awsbatch2.execute_deduplicate_bismark(samples, config, output)
        elif stage == "methylation":
            awsbatch2.execute_bismark_methylation_extractor(
                samples, config, genome, output
            )
        else:
            raise AssertionError(stage)

    def _submitted(self):
        groups = []
        pending = []
        for event in self.events:
            if event[0] == "exists":
                pending.append(event)
            else:
                groups.append((pending, event[1], event[2]))
                pending = []
        return groups, pending

    def _assert_job_matches_check(self, stage, checks, job):
        inputs = _input_paths(stage, job)
        markers = _marker(inputs)
        self.assertEqual(
            len(markers),
            1,
            "job inputs must belong to one sample directory, got %r" % (inputs,),
        )
        marker = markers[0]
        output_dir = _output_dir(job)
        self.assertTrue(
            _contains_relative(output_dir, marker),
            "output directory %r does not include %s" % (output_dir, marker),
        )
        self.assertNotIn(str(REL_ONE if marker == REL_TWO else REL_TWO), output_dir)
        expected = _expected_outputs(stage, job)
        for path in expected:
            self.assertTrue(_contains_relative(path, marker), path)
        checked = {_norm(path) for _, path, _ in checks}
        self.assertTrue(checked, "existence was not checked before submit")
        self.assertLessEqual(checked, {_norm(path) for path in expected})
        for _, path, present in checks:
            self.assertFalse(present, path)
            self.assertNotEqual(_norm(path), _norm(output_dir))
        return marker, expected

    def _assert_stage_isolated(self, stage):
        self._run(stage)
        groups, skipped = self._submitted()
        self.assertEqual(skipped, [])
        self.assertEqual(len(groups), 2, self.events)
        seen = {}
        for checks, job, _json_path in groups:
            marker, expected = self._assert_job_matches_check(stage, checks, job)
            seen[marker] = expected
        self.assertCountEqual(seen.keys(), [REL_ONE, REL_TWO])
        self.assertTrue(
            {_norm(path) for path in seen[REL_ONE]}.isdisjoint(
                {_norm(path) for path in seen[REL_TWO]}
            )
        )
        return seen

    def test_trim_outputs_keep_the_sample_directory(self):
        self._assert_stage_isolated("trim")

    def test_bismark_outputs_keep_the_sample_directory(self):
        self._assert_stage_isolated("bismark")

    def test_deduplicate_outputs_keep_the_sample_directory(self):
        self._assert_stage_isolated("dedup")

    def test_methylation_outputs_keep_the_sample_directory(self):
        self._assert_stage_isolated("methylation")

    def test_shared_basename_file_does_not_block_a_different_directory(self):
        for stage, (folder, files) in FLAT_OUTPUTS.items():
            with self.subTest(stage=stage):
                self._reset_run()
                written = []
                for name, body in files:
                    path = self.output_dir / folder / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(body)
                    written.append((path, body))
                seen = self._assert_stage_isolated(stage)
                self.assertTrue(
                    {_norm(path) for path in seen[REL_ONE]}.isdisjoint(
                        {_norm(path) for _, _ in written}
                    )
                )
                for path, body in written:
                    self.assertEqual(path.read_bytes(), body)

    def test_existing_output_suppresses_only_that_relative_path(self):
        for stage in ("trim", "bismark", "dedup", "methylation"):
            with self.subTest(stage=stage):
                self._reset_run()
                expected_by_sample = self._assert_stage_isolated(stage)
                for path in expected_by_sample[REL_ONE]:
                    Path(path).parent.mkdir(parents=True, exist_ok=True)
                    Path(path).write_bytes(b"processed-batch-one")
                self.events = []
                self._run(stage)
                groups, pending = self._submitted()
                self.assertEqual(len(groups), 1, self.events)
                checks, job, _json_path = groups[0]
                # A sample that was skipped sits in the same loop, so its
                # existence checks can land in this bucket or after the submit.
                two_checks = [
                    event
                    for event in checks
                    if _contains_relative(event[1], REL_TWO)
                ]
                one_checks = [
                    event
                    for event in list(checks) + list(pending)
                    if event[0] == "exists" and _contains_relative(event[1], REL_ONE)
                ]
                marker, _expected = self._assert_job_matches_check(
                    stage, two_checks, job
                )
                self.assertEqual(marker, REL_TWO)
                self.assertTrue(one_checks, self.events)
                self.assertTrue(all(event[2] for event in one_checks))
                self.assertLessEqual(
                    {_norm(event[1]) for event in one_checks},
                    {_norm(path) for path in expected_by_sample[REL_ONE]},
                )
                for pair, body in ((REL_ONE, b"alpha"), (REL_TWO, b"beta")):
                    directory = self.data_dir / pair
                    self.assertEqual((directory / READ_ONE).read_bytes(), body + b"-r1")
                    self.assertEqual((directory / READ_TWO).read_bytes(), body + b"-r2")

    def test_upload_publishes_each_relative_path_under_its_own_key(self):
        self._run("methylation")
        groups, skipped = self._submitted()
        self.assertEqual(skipped, [])
        self.assertEqual(len(groups), 2, self.events)
        bodies = {REL_ONE: b"report-batch-one", REL_TWO: b"report-batch-two"}
        expected_by_sample = {}
        for checks, job, _json_path in groups:
            marker, expected = self._assert_job_matches_check(
                "methylation", checks, job
            )
            self.assertEqual(len(expected), 1)
            path = Path(expected[0])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(bodies[marker])
            expected_by_sample[marker] = path

        recorder = _RecordingS3()

        def client(name, *args, **kwargs):
            self.assertEqual(name, "s3")
            return recorder

        awsbatch2.boto3.client = client
        awsbatch2.upload_new_reports_to_S3(str(self.output_dir), BUCKET)

        self.assertEqual(len(recorder.uploads), 2, recorder.uploads)
        by_marker = {}
        report_root = self.output_dir / "out_report"
        for upload in recorder.uploads:
            self.assertEqual(upload["bucket"], BUCKET)
            matched = [marker for marker in bodies if _contains_relative(upload["key"], marker)]
            self.assertEqual(len(matched), 1, upload["key"])
            marker = matched[0]
            by_marker[marker] = upload
            relative_key = os.path.relpath(expected_by_sample[marker], report_root)
            self.assertEqual(_norm(upload["key"]), _norm(relative_key))
            self.assertEqual(upload["body"], bodies[marker])
            self.assertEqual(_norm(upload["filename"]), _norm(expected_by_sample[marker]))
            self.assertTrue(expected_by_sample[marker].is_file())
        self.assertCountEqual(by_marker.keys(), [REL_ONE, REL_TWO])
        self.assertNotEqual(by_marker[REL_ONE]["key"], by_marker[REL_TWO]["key"])
        self.assertEqual(
            {head["key"] for head in recorder.heads},
            {upload["key"] for upload in recorder.uploads},
        )

    def test_main_uses_the_input_directory_for_every_step(self):
        recorder = _RecordingS3()

        def client(name, *args, **kwargs):
            self.assertEqual(name, "s3")
            return recorder

        awsbatch2.boto3.client = client
        argv = [
            "awsbatch2.py",
            "--data_dir",
            str(self.data_dir),
            "--config_dir",
            str(self.config_dir),
            "--genome_dir",
            str(self.genome_dir),
            "--output_dir",
            str(self.output_dir),
            "--s3_bucket",
            BUCKET,
        ]
        old_argv = sys.argv
        sys.argv = argv
        try:
            awsbatch2.main()
        finally:
            sys.argv = old_argv

        groups, skipped = self._submitted()
        self.assertEqual(skipped, [])
        by_stage = {"trim": [], "bismark": [], "dedup": [], "methylation": []}
        for checks, job, json_path in groups:
            name = Path(json_path).name
            if name.endswith("-trim_galore.json"):
                stage = "trim"
            elif name.endswith("-bismark.json"):
                stage = "bismark"
            elif name.endswith("-deduplicate_bismark.json"):
                stage = "dedup"
            elif name.endswith("-bismark_methylation_extractor.json"):
                stage = "methylation"
            else:
                self.fail(name)
            by_stage[stage].append((checks, job, json_path))
        for stage, stage_groups in by_stage.items():
            self.assertEqual(len(stage_groups), 2, stage)
            seen = set()
            for checks, job, _json_path in stage_groups:
                marker, _expected = self._assert_job_matches_check(stage, checks, job)
                seen.add(marker)
            self.assertEqual(seen, {REL_ONE, REL_TWO})


if __name__ == "__main__":
    unittest.main()
