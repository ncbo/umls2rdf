import io
import os
import tarfile
import tempfile
import unittest

from download_semantic_network import extract_semantic_network


def make_archive(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    buf.seek(0)
    return buf


class ExtractSemanticNetworkTest(unittest.TestCase):
    def test_saves_srdef_and_srstr_only(self):
        archive = make_archive({"2023AA/SRDEF": b"def", "2023AA/SRSTR": b"str",
                                "2023AA/SU": b"su", "../SRDEF.bak": b"x"})
        with tempfile.TemporaryDirectory() as dest:
            release = extract_semantic_network(archive, os.path.join(dest, "sn"))
            self.assertEqual(release, "2023AA")
            self.assertEqual(sorted(os.listdir(os.path.join(dest, "sn"))), ["SRDEF", "SRSTR"])
            with open(os.path.join(dest, "sn", "SRSTR"), "rb") as f:
                self.assertEqual(f.read(), b"str")

    def test_missing_file_fails_before_writing(self):
        archive = make_archive({"2023AA/SRDEF": b"def"})
        with tempfile.TemporaryDirectory() as dest:
            with self.assertRaisesRegex(ValueError, "SRSTR"):
                extract_semantic_network(archive, os.path.join(dest, "sn"))
            self.assertFalse(os.path.exists(os.path.join(dest, "sn")))


if __name__ == "__main__":
    unittest.main()
