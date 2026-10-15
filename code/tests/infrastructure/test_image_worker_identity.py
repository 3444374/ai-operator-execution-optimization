"""Strict actual Torch/NVML GPU identity checks before CLIP weights load."""
import unittest
from unittest.mock import Mock
from src.modalities.image.worker_identity import assigned_gpu_uuid


class ImageWorkerIdentityTests(unittest.TestCase):
    def test_worker_checks_actual_gpu_before_model_load(self):
        from types import SimpleNamespace
        a = "GPU-00000000-1111-2222-3333-444444444440"
        b = "GPU-00000000-1111-2222-3333-444444444441"
        torch = SimpleNamespace(cuda=SimpleNamespace(device_count=lambda: 1,
            get_device_properties=Mock(return_value=SimpleNamespace(uuid=b[4:]))))
        self.assertEqual(assigned_gpu_uuid(torch, "cuda:0", (a, b)), b)
        with self.assertRaisesRegex(ValueError, "outside"):
            assigned_gpu_uuid(torch, "cuda", (a,))
        for count in (0, 2):
            torch.cuda.device_count = lambda: count
            torch.cuda.get_device_properties.reset_mock()
            with self.subTest(count=count), self.assertRaisesRegex(ValueError, "exactly one"):
                assigned_gpu_uuid(torch, "cuda", (a, b))
            torch.cuda.get_device_properties.assert_not_called()

    def test_torch_uuid_object_and_nvml_spellings_match_one_physical_device(self):
        from types import SimpleNamespace
        token = "GPU-12345678-abcd-4321-9876-0123456789ab"

        class TorchUuid:
            def __str__(self):
                return token[4:]

        for value in (TorchUuid(), token[4:], token, token.upper()):
            torch = SimpleNamespace(cuda=SimpleNamespace(device_count=lambda: 1,
                get_device_properties=lambda device: SimpleNamespace(uuid=value)))
            with self.subTest(value=str(value)):
                self.assertEqual(assigned_gpu_uuid(torch, "cuda", (token,)), token)
                self.assertEqual(assigned_gpu_uuid(torch, "cuda", (token.upper(),)), token.upper())

    def test_worker_uuid_normalization_rejects_malformed_and_duplicate_identities(self):
        from types import SimpleNamespace
        token = "GPU-12345678-abcd-4321-9876-0123456789ab"
        values = (None, b'12345678-abcd-4321-9876-0123456789ab', "GPU-a", "",
            token[4:].replace("-", ""), "{" + token[4:] + "}", "urn:uuid:" + token[4:],
            " " + token, token + " ", token + "\n", token + "\x00", "prefix" + token,
            token + "suffix", "GPU-" + token, "MIG-" + token[4:], token.replace("ab", "g1"),
            token.replace("1", "１", 1))
        for value in values:
            torch = SimpleNamespace(cuda=SimpleNamespace(device_count=lambda: 1,
                get_device_properties=lambda device: SimpleNamespace(uuid=value)))
            with self.subTest(value=repr(value)), self.assertRaisesRegex(ValueError, "UUID"):
                assigned_gpu_uuid(torch, "cuda", (token,))
        torch.cuda.get_device_properties = lambda device: SimpleNamespace(uuid=token[4:])
        for allowed in ((token, token.upper()), (token[4:],), ("GPU-a",)):
            with self.subTest(allowed=allowed), self.assertRaisesRegex(ValueError, "UUID"):
                assigned_gpu_uuid(torch, "cuda", allowed)
