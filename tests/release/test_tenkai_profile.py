import unittest

from tests.release import load

profile = load("tenkai_profile")


class TenkaiProfile(unittest.TestCase):
    def test_committed_template_and_readme_pass_the_release_check(self):
        # check() renders a sample profile from the committed template and
        # fails on a wrong pin, an accepted bad digest, or a version mismatch.
        profile.check()


if __name__ == "__main__":
    unittest.main()
