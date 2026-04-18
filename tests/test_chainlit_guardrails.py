from agents.guardrails import is_natural_gas_question
import unittest


class GuardrailTests(unittest.TestCase):
    def test_guardrail_allows_natural_gas_questions(self) -> None:
        self.assertTrue(is_natural_gas_question("Who are the top 5 gas producers?"))
        self.assertTrue(is_natural_gas_question("How much gas is in storage?"))
        self.assertTrue(is_natural_gas_question("Are LNG exports higher than last year?"))

    def test_guardrail_blocks_non_gas_energy_questions(self) -> None:
        self.assertFalse(is_natural_gas_question("What is the outlook for crude oil?"))
        self.assertFalse(is_natural_gas_question("Top 5 solar companies"))
        self.assertFalse(is_natural_gas_question("How much uranium does the US produce?"))

    def test_guardrail_allows_followup_with_natural_gas_context(self) -> None:
        self.assertTrue(
            is_natural_gas_question(
                "top 5 countries",
                "Who are the top 5 gas producers?",
            )
        )

    def test_guardrail_blocks_followup_that_switches_to_other_energy(self) -> None:
        self.assertFalse(
            is_natural_gas_question(
                "what about crude oil?",
                "Who are the top 5 gas producers?",
            )
        )


if __name__ == "__main__":
    unittest.main()
