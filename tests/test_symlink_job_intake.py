"""Spec for sequencing intake: do not queue symlink read pairs.

These tests call the real intake in source/awsbatch2.py. They fail while a
pair of symlinks is walked, written into a job JSON, and submitted. A fix
passes only when every stage refuses that pair before it writes a job file
or submits it, including when the directory entry is replaced by a symlink
after discovery and before queueing. Queued FASTQ paths must still be the
regular files that were accepted: opening them must not follow a symlink
to another sample.
"""

import builtins
import json
import os
import stat
import sys
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source"))

import awsbatch2  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402


VICTIM_ONE = b"VICTIM-SAMPLE-ONE"
VICTIM_TWO = b"VICTIM-SAMPLE-TWO"
INSIDE_ONE = b"INSIDE-SAMPLE-ONE"
INSIDE_TWO = b"INSIDE-SAMPLE-TWO"


class _JsonWrite:
    def __init__(self, raw, sink):
        self._raw = raw
        self._sink = sink

    def write(self, data):
        self._sink.append(data)
        return self._raw.write(data)

    def __getattr__(self, name):
        return getattr(self._raw, name)

    def __enter__(self):
        self._raw.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._raw.__exit__(exc_type, exc, tb)


def _install_fake_aws(tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    log_path = tmp_path / "aws-invocations.log"
    script = bindir / "aws"
    script.write_text(
        "#!/bin/sh\n"
        'printf "%s\\n" "$*" >> "$AWS_INTAKE_LOG"\n'
        "exit 0\n"
    )
    script.chmod(0o755)
    monkeypatch.setenv("AWS_INTAKE_LOG", str(log_path))
    monkeypatch.setenv(
        "PATH", str(bindir) + os.pathsep + os.environ.get("PATH", "")
    )
    return log_path


def _run_intake(tmp_path, monkeypatch, data_dir):
    config_dir = tmp_path / "config"
    output_dir = tmp_path / "output"
    genome_dir = tmp_path / "genome"
    config_dir.mkdir()
    output_dir.mkdir()
    genome_dir.mkdir()
    log_path = _install_fake_aws(tmp_path, monkeypatch)
    written = []
    real_open = builtins.open

    def tracking_open(file, mode="r", *args, **kwargs):
        handle = real_open(file, mode, *args, **kwargs)
        mode_text = mode if isinstance(mode, str) else ""
        try:
            name = os.fspath(file)
        except TypeError:
            name = ""
        if "w" in mode_text and str(name).endswith(".json"):
            return _JsonWrite(handle, written)
        return handle

    monkeypatch.setattr(builtins, "open", tracking_open)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "awsbatch2.py",
            "--data_dir",
            str(data_dir),
            "--config_dir",
            str(config_dir),
            "--genome_dir",
            str(genome_dir),
            "--output_dir",
            str(output_dir),
            "--s3_bucket",
            "results-bucket",
        ],
    )
    awsbatch2.main()
    invocations = (
        log_path.read_text().splitlines() if log_path.exists() else []
    )
    return config_dir, output_dir, written, invocations


def _write_pair(directory, stem, first, second):
    directory.mkdir(parents=True, exist_ok=True)
    left = directory / f"{stem}_1.fastq.gz"
    right = directory / f"{stem}_2.fastq.gz"
    left.write_bytes(first)
    right.write_bytes(second)
    return left, right


def _link_pair(directory, stem, target_left, target_right, relative=False):
    directory.mkdir(parents=True, exist_ok=True)
    left = directory / f"{stem}_1.fastq.gz"
    right = directory / f"{stem}_2.fastq.gz"
    if relative:
        left.symlink_to(os.path.relpath(target_left, directory))
        right.symlink_to(os.path.relpath(target_right, directory))
    else:
        left.symlink_to(target_left)
        right.symlink_to(target_right)
    return left, right


def _command_tokens(config_dir):
    tokens = []
    for path in sorted(config_dir.glob("*.json")):
        payload = json.loads(path.read_text())
        command = payload["containerOverrides"]["command"]
        for part in command:
            tokens.extend(part.split())
    return tokens


def _fastq_tokens(config_dir):
    found = []
    for token in _command_tokens(config_dir):
        if token.endswith(".fastq.gz") or token.endswith(".fq.gz"):
            found.append(token)
    return found


def _assert_victim_intact(path, payload):
    assert path.exists()
    assert not path.is_symlink()
    assert stat.S_ISREG(path.stat().st_mode)
    assert path.read_bytes() == payload


def _assert_queued_fastqs_are_pinned(config_dir, input_dir, forbidden_bytes):
    """Open every FASTQ path the intake placed in a job command.

    A normal open follows symlinks, which is how the batch container reads
    the path it was given. O_NOFOLLOW then requires that directory entry to
    be the regular file that was accepted. Either check fails when the
    command names a symlink, including one swapped in after discovery.
    """
    input_real = os.path.realpath(input_dir)
    for token in _fastq_tokens(config_dir):
        assert os.path.isabs(token)
        followed = Path(token).read_bytes()
        for marker in forbidden_bytes:
            assert marker not in followed, followed
        try:
            fd = os.open(token, os.O_NOFOLLOW | os.O_RDONLY)
        except OSError as exc:
            raise AssertionError(
                f"queued FASTQ {token} is not a pinned regular file ({exc}); "
                f"a following open read {followed!r}"
            ) from exc
        try:
            st = os.fstat(fd)
            assert stat.S_ISREG(st.st_mode)
            chunks = []
            while True:
                block = os.read(fd, 1024 * 1024)
                if not block:
                    break
                chunks.append(block)
            pinned = b"".join(chunks)
        finally:
            os.close(fd)
        assert pinned == followed
        resolved = os.path.realpath(token)
        assert os.path.commonpath([input_real, resolved]) == input_real


def _assert_absent(written, config_dir, needles):
    blob = "".join(part if isinstance(part, str) else part.decode() for part in written)
    on_disk = "\n".join(path.read_text() for path in config_dir.glob("*.json"))
    for needle in needles:
        assert needle not in blob
        assert needle not in on_disk


def test_regular_pair_is_queued_for_every_stage(tmp_path, monkeypatch):
    data_dir = tmp_path / "input"
    _write_pair(data_dir, "library", INSIDE_ONE, INSIDE_TWO)
    config_dir, output_dir, written, invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    names = sorted(path.name for path in config_dir.glob("*.json"))
    assert names == [
        "1-bismark.json",
        "1-bismark_methylation_extractor.json",
        "1-deduplicate_bismark.json",
        "1-trim_galore.json",
    ]
    assert len(invocations) == 4
    queued = _fastq_tokens(config_dir)
    assert queued == [
        str(data_dir / "library_1.fastq.gz"),
        str(data_dir / "library_2.fastq.gz"),
        str(data_dir / "library_1.fastq.gz"),
        str(data_dir / "library_2.fastq.gz"),
    ]
    _assert_queued_fastqs_are_pinned(config_dir, data_dir, forbidden_bytes=())
    assert list(output_dir.rglob("*")) == []
    assert written


def test_absolute_symlink_pair_is_not_queued(tmp_path, monkeypatch):
    data_dir = tmp_path / "input"
    other = tmp_path / "other-sample"
    victim_left, victim_right = _write_pair(
        other, "secret", VICTIM_ONE, VICTIM_TWO
    )
    _link_pair(data_dir, "attacker", victim_left, victim_right)
    _write_pair(data_dir, "library", INSIDE_ONE, INSIDE_TWO)

    config_dir, output_dir, written, invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    _assert_absent(
        written,
        config_dir,
        [
            "attacker_1.fastq.gz",
            "attacker_2.fastq.gz",
            str(victim_left),
            str(victim_right),
            "secret_1.fastq.gz",
            "secret_2.fastq.gz",
        ],
    )
    _assert_queued_fastqs_are_pinned(
        config_dir, data_dir, forbidden_bytes=(VICTIM_ONE, VICTIM_TWO)
    )
    assert sorted(path.name for path in config_dir.glob("*.json")) == [
        "1-bismark.json",
        "1-bismark_methylation_extractor.json",
        "1-deduplicate_bismark.json",
        "1-trim_galore.json",
    ]
    assert len(invocations) == 4
    assert list(output_dir.rglob("*")) == []
    _assert_victim_intact(victim_left, VICTIM_ONE)
    _assert_victim_intact(victim_right, VICTIM_TWO)


def test_relative_nested_and_inside_symlinks_are_not_queued(tmp_path, monkeypatch):
    data_dir = tmp_path / "input"
    other = tmp_path / "other-sample"
    victim_left, victim_right = _write_pair(
        other, "secret", VICTIM_ONE, VICTIM_TWO
    )
    _link_pair(
        data_dir,
        "relative",
        victim_left,
        victim_right,
        relative=True,
    )
    nested = data_dir / "batch" / "run"
    _link_pair(nested, "nested", victim_left, victim_right)
    hidden_left, hidden_right = _write_pair(
        data_dir, "hidden", INSIDE_ONE, INSIDE_TWO
    )
    _link_pair(data_dir, "alias", hidden_left, hidden_right)
    # One mate is a regular file inside the input directory and the other
    # is a symlink to the other sample. The pair is still untrusted.
    mate_dir = data_dir / "mixed"
    mate_dir.mkdir(parents=True)
    (mate_dir / "mixed_1.fastq.gz").write_bytes(INSIDE_ONE)
    (mate_dir / "mixed_2.fastq.gz").symlink_to(victim_right)

    config_dir, output_dir, written, invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    _assert_absent(
        written,
        config_dir,
        [
            "relative_1.fastq.gz",
            "relative_2.fastq.gz",
            "nested_1.fastq.gz",
            "nested_2.fastq.gz",
            "alias_1.fastq.gz",
            "alias_2.fastq.gz",
            "mixed_1.fastq.gz",
            "mixed_2.fastq.gz",
            str(victim_left),
            str(victim_right),
        ],
    )
    _assert_queued_fastqs_are_pinned(
        config_dir, data_dir, forbidden_bytes=(VICTIM_ONE, VICTIM_TWO)
    )
    # The only regular pair is hidden_*.fastq.gz, and it is queued once
    # per stage that reads the reads (trim and bismark).
    assert _fastq_tokens(config_dir) == [
        str(hidden_left),
        str(hidden_right),
        str(hidden_left),
        str(hidden_right),
    ]
    assert len(invocations) == 4
    assert list(output_dir.rglob("*")) == []
    _assert_victim_intact(victim_left, VICTIM_ONE)
    _assert_victim_intact(victim_right, VICTIM_TWO)


def test_directory_symlink_is_not_treated_as_a_sample_directory(tmp_path, monkeypatch):
    data_dir = tmp_path / "input"
    data_dir.mkdir()
    other = tmp_path / "other-sample"
    _write_pair(other, "secret", VICTIM_ONE, VICTIM_TWO)
    (data_dir / "linked-batch").symlink_to(other, target_is_directory=True)

    config_dir, output_dir, written, invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    assert list(config_dir.glob("*.json")) == []
    assert invocations == []
    assert written == []
    assert list(output_dir.rglob("*")) == []
    assert _fastq_tokens(config_dir) == []


def test_symlink_swap_after_discovery_is_not_queued(tmp_path, monkeypatch):
    """Replace the discovered pair with symlinks before any job is written.

    main() discovers samples, then each stage queues from the returned path
    strings. Swapping the directory entries in between is the attacker
    replacing a regular file after a check that only looked at the walk.
    Re-checking the same path string later, then putting that string in the
    command, still fails: the queued path is opened with O_NOFOLLOW.
    """
    data_dir = tmp_path / "input"
    other = tmp_path / "other-sample"
    victim_left, victim_right = _write_pair(
        other, "secret", VICTIM_ONE, VICTIM_TWO
    )
    left, right = _write_pair(data_dir, "raced", INSIDE_ONE, INSIDE_TWO)

    real_get_samples = awsbatch2.get_samples

    def discover_then_swap(directory):
        found = real_get_samples(directory)
        for link, target in ((left, victim_left), (right, victim_right)):
            os.remove(link)
            os.symlink(target, link)
        return found

    monkeypatch.setattr(awsbatch2, "get_samples", discover_then_swap)
    config_dir, output_dir, written, _invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    _assert_absent(
        written,
        config_dir,
        [str(victim_left), str(victim_right)],
    )
    _assert_queued_fastqs_are_pinned(
        config_dir, data_dir, forbidden_bytes=(VICTIM_ONE, VICTIM_TWO)
    )
    assert list(output_dir.rglob("*")) == []
    _assert_victim_intact(victim_left, VICTIM_ONE)
    _assert_victim_intact(victim_right, VICTIM_TWO)
    # Refusing the pair must not delete the attacker's directory entries.
    assert left.is_symlink() and right.is_symlink()


class _RecordingS3:
    """Stand-in for the S3 client upload_new_reports_to_S3 calls.

    upload_file reads the local path with a normal open, as
    s3transfer's ReadFileChunk.from_filename does, and records those bytes.
    head_object reports the object missing so the 404 branch uploads it.
    """

    def __init__(self):
        self.uploads = []

    def head_object(self, Bucket, Key):
        raise ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

    def upload_file(self, Filename, Bucket, Key, **kwargs):
        with open(Filename, "rb") as handle:
            body = handle.read()
        self.uploads.append((Bucket, Key, body))


def test_queued_symlink_reads_are_not_uploaded(tmp_path, monkeypatch):
    data_dir = tmp_path / "input"
    other = tmp_path / "other-sample"
    victim_left, victim_right = _write_pair(
        other, "secret", VICTIM_ONE, VICTIM_TWO
    )
    _link_pair(data_dir, "attacker", victim_left, victim_right)
    config_dir, output_dir, _written, _invocations = _run_intake(
        tmp_path, monkeypatch, data_dir
    )

    # The batch jobs are asynchronous. A later intake uploads whatever
    # CX report those jobs left under out_report. The report body here is
    # exactly what a following open of each queued FASTQ returned.
    report_dir = output_dir / "out_report"
    for token in _fastq_tokens(config_dir):
        body = Path(token).read_bytes()
        if VICTIM_ONE not in body and VICTIM_TWO not in body:
            continue
        deduplicated = awsbatch2.create_deduplicate_bismark_output(
            token, str(output_dir / "out_deduplicate")
        )
        report = awsbatch2.create_bismark_methylation_extractor_output(
            deduplicated, str(report_dir)
        )
        Path(report).parent.mkdir(parents=True, exist_ok=True)
        Path(report).write_bytes(body)

    recorder = _RecordingS3()

    def client(service, *args, **kwargs):
        assert service == "s3"
        return recorder

    monkeypatch.setattr(awsbatch2.boto3, "client", client)
    awsbatch2.upload_new_reports_to_S3(str(output_dir), "results-bucket")
    uploaded = b"".join(body for _bucket, _key, body in recorder.uploads)
    assert VICTIM_ONE not in uploaded
    assert VICTIM_TWO not in uploaded
    _assert_victim_intact(victim_left, VICTIM_ONE)
    _assert_victim_intact(victim_right, VICTIM_TWO)
